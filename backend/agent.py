import asyncio
import os
import logging
import sys
from livekit import rtc
from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli, llm
from livekit.plugins import openai, silero
from api import GermanTeacherTools
from openai_manager import system_message
from dotenv import load_dotenv
from datetime import datetime
from database import get_db_connection_context # Імпорт контекстного менеджера для підключення до БД
from livekit.agents.voice import room_io


load_dotenv()

# =========================
# GLOBAL SINGLETON OBJECTS
# =========================

_LLM = None
_STT = None
_TTS = None
_VAD = None

def get_pipeline_components():
    """
    Create STT/LLM/TTS/VAD once and reuse for all jobs.
    This removes per-job cold start overhead.
    """
    global _LLM, _STT, _TTS, _VAD

    if _LLM is not None and _STT is not None and _TTS is not None and _VAD is not None:
        return _LLM, _STT, _TTS, _VAD

    # --- LLM / STT / TTS KEYS ---
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")

    # Create once:
    _LLM = openai.LLM(model="gpt-4o", api_key=api_key)
    _STT = openai.STT(model="whisper-1", language="de")
    _TTS = openai.TTS(model="tts-1", voice="alloy")

    # Create once (this is often the slow one):
    _VAD = silero.VAD.load(
        min_speech_duration=0.1,
        min_silence_duration=0.3,  # was 0.5 -> faster "end of speech"
    )

    return _LLM, _STT, _TTS, _VAD


# === НАСТРОЙКА ЛОГИРОВАНИЯ ===
if not os.path.exists("logs"):
    os.makedirs("logs")

IS_LIVEKIT_DEV_PARENT = (os.getenv("LIVEKIT_WATCH_PARENT") == "1")

# Настройка логгера
if not IS_LIVEKIT_DEV_PARENT:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [pid=%(process)d] - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("agent.log", encoding="utf-8")
        ],
        force=True
    )
    logging.info("📢 TEST LOG ENTRY: Logging system initialized successfully!")
else:
    # parent-процесс (watcher) — не настраиваем логирование и не шумим
    pass

# === FIX: убираем дубли логов от livekit ===
for name in ("livekit", "livekit.agents"):
    lg = logging.getLogger(name)
    lg.handlers.clear()     # убираем собственные handlers livekit
    lg.propagate = True     # пусть сообщения идут в root (который настроен basicConfig)


class NoBinaryFilter(logging.Filter):
    def filter(self, record):
        return not isinstance(record.msg, bytes)

logging.getLogger().addFilter(NoBinaryFilter())

# === ФУНКЦИЯ ЗАПИСИ ТРАНСКРИПТА ===
def save_transcript(role, text):
    """Записывает реплики в файл conversation.txt"""
    try:
        with open("logs/conversation.txt", "a", encoding="utf-8") as f:
            time_str = datetime.now().strftime("%H:%M:%S")
            f.write(f"[{time_str}] {role}: {text}\n")
    except Exception as e:
        logging.error(f"Ошибка записи транскрипта: {e}")

# === КЛАСС АГЕНТА ===
class GermanTeacherAgent(Agent):
    def __init__(self, llm_instance):
        super().__init__(instructions=system_message["german_teacher_instructions"])
        self.chat_model = llm_instance
        self.current_instructions = self.instructions

        # Эти поля реально используются твоими wrapper-логиками
        self.current_user_id = None
        self.user_name = "Student"  # Имя по умолчанию


    def fetch_user_name(self, user_id):
        """Прямой запрос к базе данных за именем"""
        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Берем самое свежее имя из таблицы прогресса
                    cursor.execute("SELECT username FROM bt_3_user_progress WHERE user_id = %s ORDER BY start_time DESC LIMIT 1;", (user_id,))
                    result = cursor.fetchone()
                    if result and result[0]:
                        return result[0]
        except Exception as e:
            logging.error(f"❌ Ошибка при поиске имени в БД: {e}")
        return None

from typing import Optional

# === ТОЧКА ВХОДА ===
async def entrypoint(ctx: JobContext):
    logging.info("✨ Starting German Teacher Agent...")

    # api_key = os.environ.get("OPENAI_API_KEY")
    # if not api_key:
    #     logging.error("❌ OPENAI_API_KEY is not set")
    #     return

    # 1) Подключаемся к комнате
    await ctx.connect()
    logging.info("✅ Connected to the room. Waiting for participants...")

    # === DISCONNECT TIMEOUT LOGIC ===
    DISCONNECT_TIMEOUT_SEC = 30

    disconnect_task: Optional[asyncio.Task] = None
    stop_event = asyncio.Event()

    def _has_remote_participants() -> bool:
        """Проверяем, есть ли сейчас хоть один remote participant."""
        rp = getattr(ctx.room, "remote_participants", None)
        if isinstance(rp, dict):
            return len(rp) > 0
        if rp is None:
            return False
        try:
            return len(list(rp)) > 0
        except Exception:
            return False
        
    async def _close_session_after_timeout(reason: str):
        logging.warning(f"⏳ Disconnect detected. Will close session in {DISCONNECT_TIMEOUT_SEC}s if user doesn't return. Reason={reason}")
        
        try:
            await asyncio.sleep(DISCONNECT_TIMEOUT_SEC)

            # Если за это время кто-то снова подключился — не закрываем
            # Считаем "вернулся", если есть хоть один remote participant

            if _has_remote_participants():
                logging.info("✅ Participant returned within timeout — keeping session alive.")
                return

            logging.warning("🧨 No participant returned — closing AgentSession now.")
            try:
                # Это команда библиотеке LiveKit: "Закрывай лавочку". 
                # Отключись от сервера, разорви соединение с OpenAI.
                await session.aclose()
                stop_event.set()
                logging.info("✅ Session closed due to participant absence.")
            except Exception as e:
                logging.error(f"❌ Failed to close session cleanly: {e}", exc_info=True)

        except asyncio.CancelledError:
            logging.info("✅ Disconnect timeout task cancelled (participant returned).")
            return

    # 2) Create / reuse pipeline components (singleton)
    try:
        my_llm, my_stt, my_tts, my_vad = get_pipeline_components()
    except Exception as e:
        logging.error(f"❌ Failed to init pipeline components: {e}", exc_info=True)
        return

    # 3) Наша бизнес-логика (пока оставляем класс как есть)
    teacher_logic = GermanTeacherAgent(llm_instance=my_llm)
    teacher_logic._greeted_user_ids = set() # Для анти-спама приветствий

    # Отримуємо SID сесії (унікальний ID дзвінка)
    # ctx.room.sid - це унікальний ідентифікатор саме цієї сесії розмови
    # Tools: берем уже готовые FunctionTool из GermanTeacherTools (@llm.function_tool)

    # ✅ SID: у тебя было "<coroutine object Room.sid ...>"
    # Значит sid - async (либо sid(), либо property, который возвращает coroutine).
    sid_obj = getattr(ctx.room, "sid", None)
    if callable(sid_obj):
        sid_obj = sid_obj()  # если это method -> получим coroutine/значение
    session_id = await sid_obj if asyncio.iscoroutine(sid_obj) else sid_obj
    logging.info(f"🎯 Session ID: {session_id}")
    
    teacher_tools_instance = GermanTeacherTools(session_id=session_id)

    # 5) Tool-wrapper без аргументов, который достает user_id 
    @llm.function_tool
    async def get_recent_telegram_mistakes() -> str:
        # 1) если participant_connected уже успел — current_user_id уже есть
        if not teacher_logic.current_user_id:
            ok = await _resolve_user_id_from_room()
            if not ok:
                return "User ID is not set yet (no participant identified)."


        # 2) если имя ещё не подтянуто — подтянем (на всякий)
        if not teacher_logic.user_name or teacher_logic.user_name == "Student":
            real_name = teacher_logic.fetch_user_name(teacher_logic.current_user_id)
            teacher_logic.user_name = real_name or "Student"

        return await teacher_tools_instance.get_recent_telegram_mistakes(
            user_id=teacher_logic.current_user_id
        )
    


    # FALLBACK (not used currently)
    async def _resolve_user_id_from_room() -> bool:
        """
        Пытается определить Telegram user_id по участникам в комнате.
        Делает это один раз и сохраняет в teacher_logic.current_user_id.
        Также подтягивает имя из БД и приветствует ОДИН раз.
        Возвращает True если получилось определить user_id, иначе False.
        """

        # 0) Если уже определили — ничего не делаем
        if teacher_logic.current_user_id:
            return True

        # 1) Достаём участников из комнаты максимально совместимо
        room = ctx.room
        participants = []

        # Вариант A: remote_participants (часто dict)
        rp = getattr(room, "remote_participants", None)
        if rp:
            if isinstance(rp, dict):
                participants.extend(list(rp.values()))
            else:
                participants.extend(list(rp))

        # Вариант B: participants (иногда общий список/словарь)
        p = getattr(room, "participants", None)
        if p:
            if isinstance(p, dict):
                participants.extend(list(p.values()))
            else:
                participants.extend(list(p))

        # Убираем None и дубли
        participants = [x for x in participants if x is not None]
        if not participants:
            logging.warning("❌ Cannot resolve user_id: no participants in room yet.")
            return False

        # 2) Ищем первого участника с числовым identity
        for part in participants:
            identity = getattr(part, "identity", None)
            if not identity:
                continue

            try:
                user_id_int = int(identity)
            except Exception:
                continue

            # ✅ Нашли
            teacher_logic.current_user_id = user_id_int
            logging.info(f"✅ Resolved user_id from room participants: {teacher_logic.current_user_id}")

            # 3) Подтягиваем имя из БД
            real_name = teacher_logic.fetch_user_name(teacher_logic.current_user_id)
            teacher_logic.user_name = real_name or "Student"
            logging.info(f"✅ Resolved username: {teacher_logic.user_name}")

            # 4) Анти-спам приветствия (один раз на user_id)
            if teacher_logic.current_user_id not in teacher_logic._greeted_user_ids:
                teacher_logic._greeted_user_ids.add(teacher_logic.current_user_id)

                # Обновим instructions (если ты их реально используешь дальше)
                teacher_logic.current_instructions = (
                    f"{system_message['german_teacher_instructions']}\n\n"
                    f"--- CONTEXT UPDATE ---\n"
                    f"CURRENT STUDENT NAME: {teacher_logic.user_name}\n"
                    f"CURRENT STUDENT ID: {teacher_logic.current_user_id}\n"
                    f"IMPORTANT: Always address the student by name when appropriate.\n"
                )


            return True

        logging.warning("❌ Cannot resolve user_id: no numeric participant.identity found.")
        return False

    @llm.function_tool
    async def get_student_context() -> str:
        if not teacher_logic.current_user_id:
            ok = await _resolve_user_id_from_room()
            if not ok:
                return "{'user_id': null, 'user_name': 'Student'}"

        if teacher_logic.user_name == "Student":
            real_name = teacher_logic.fetch_user_name(teacher_logic.current_user_id)
            teacher_logic.user_name = real_name or "Student"

        return str({"user_id": teacher_logic.current_user_id, "user_name": teacher_logic.user_name})


    # 6) Список tools для AgentSession. AgentSession умеет tools штатно.
    tools = [
        get_recent_telegram_mistakes, # <-- wrapper без аргументов
        get_student_context,  
        teacher_tools_instance.explain_grammar,
        teacher_tools_instance.generate_quiz_question,
        teacher_tools_instance.evaluate_quiz_answer,
        teacher_tools_instance.bookmark_phrase,
        teacher_tools_instance.log_conversation_mistake
]

    # 7) Создаем AgentSession с нашими компонентами
    # Старые версии SDK не поддерживают tools=..., поэтому делаем совместимый путь.
    try:
        session = AgentSession(
            stt=my_stt,
            llm=my_llm,
            tts=my_tts,
            vad=my_vad,
            tools=tools,
            allow_interruptions=True,
        )
    except TypeError:
        try:
            my_llm = my_llm.with_tools(tools)
        except Exception as exc:
            logging.warning(f"⚠️ LLM does not support tools(): {exc}")
        session = AgentSession(
            stt=my_stt,
            llm=my_llm,
            tts=my_tts,
            vad=my_vad,
            allow_interruptions=True,
        )

    # HANDLERS (СНАЧАЛА def, ПОТОМ .on)
    def on_participant_connected(participant: rtc.RemoteParticipant):
            nonlocal disconnect_task
            logging.info("=============================================")
            logging.info("👋 participant_connected")
            logging.info(f"🆔 Identity: '{getattr(participant, 'identity', None)}'")
            logging.info(f"👤 Name(token): '{getattr(participant, 'name', None)}'")
            logging.info("=============================================")

            if disconnect_task and not disconnect_task.done():
                disconnect_task.cancel()

            identity = getattr(participant, "identity", None)
            if not identity:
                asyncio.create_task(session.say("Hallo! Entschuldigung, ich kann deine ID nicht lesen."))
                return

            # Парсим ID
            try:
                user_id_int = int(identity)
            except Exception:
                teacher_logic.current_user_id = None
                asyncio.create_task(session.say("Hallo! Entschuldigung, ich kann deine ID nicht lesen."))
                return

            # анти-спам приветствия
            if user_id_int in teacher_logic._greeted_user_ids:
                logging.info(f"👋 User {user_id_int} already greeted -> skip greeting")
                return

            teacher_logic._greeted_user_ids.add(user_id_int)
            teacher_logic.current_user_id = user_id_int

            # Имя из БД
            real_name = teacher_logic.fetch_user_name(user_id_int)
            teacher_logic.user_name = real_name or "Student"
            logging.info(f"✅ participant_connected resolved username: {teacher_logic.user_name}")

            # # Обновляем инструкции (если используешь где-то)
            # teacher_logic.current_instructions = (
            #     f"{teacher_logic.instructions}\n\n"
            #     f"--- CONTEXT UPDATE ---\n"
            #     f"CURRENT STUDENT NAME: {teacher_logic.user_name}\n"
            #     f"CURRENT STUDENT ID: {teacher_logic.current_user_id}\n"
            # )
            teacher_logic.current_instructions = (
                f"{system_message['german_teacher_instructions']}\n\n"
                f"--- CONTEXT UPDATE ---\n"
                f"CURRENT STUDENT NAME: {teacher_logic.user_name}\n"
                f"CURRENT STUDENT ID: {teacher_logic.current_user_id}\n"
            )


    def on_participant_disconnected(participant: rtc.RemoteParticipant):
        nonlocal disconnect_task
        logging.info("=============================================")
        logging.info("👋 participant_disconnected")
        logging.info(f"🆔 Identity: '{getattr(participant, 'identity', None)}'")
        logging.info(f"👤 Name(token): '{getattr(participant, 'name', None)}'")
        logging.info("=============================================")

        # Если таймер уже тикает — не запускаем второй
        if disconnect_task and not disconnect_task.done():
            return

        disconnect_task = asyncio.create_task(
            _close_session_after_timeout("participant_disconnected")
        )

    # 6.1) Транскрипт: ловим все элементы диалога
    def _on_conversation_item_added(ev):
        try:
            item = getattr(ev, "item", None)
            if not item:
                return

            role = getattr(item, "role", "unknown")
            content = getattr(item, "content", [])
            text = ""

            if content:
                first = content[0]
                text = first if isinstance(first, str) else str(first)

            logging.info(f"🧩 conversation_item_added | role={role} | text={text}")

            if role == "user":
                # при первом пользовательском сообщении пытаемся резолвить ID+имя
                if not teacher_logic.current_user_id:
                    asyncio.create_task(_resolve_user_id_from_room())

            if role in ("user", "assistant"):
                save_transcript(role.capitalize(), text)

        except Exception as e:
            logging.error(f"❌ Error in conversation_item_added handler: {e}", exc_info=True)

    # 6.2) Tools executed — просто логируем (позже красиво разберём структуру)
    def _on_tools_executed(ev):
        try:
            logging.info(f"🛠️ function_tools_executed: {ev}")
        except Exception as e:
            logging.error(f"❌ Error in function_tools_executed handler: {e}", exc_info=True)    
    # “Сессия, когда у тебя произойдёт событие conversation_item_added, вызови функцию _on_conversation_item_added и передай ей объект события.”
    # 1. пользователь сказал фразу
    # 2. session добавил в историю conversation item
    # 3. session вызвал emit("conversation_item_added", ev)
    # 4. emit нашёл listeners["conversation_item_added"]
    # 5. вызвал каждый callback из списка, передав им ev

    # 6.3) Ошибки runtime
    def _on_error(ev):
        try:
            logging.error(f"💥 session error: {ev}")
        except Exception as e:
            logging.error(f"❌ Error in error handler: {e}", exc_info=True)

    session.on("conversation_item_added", _on_conversation_item_added)

    session.on("function_tools_executed", _on_tools_executed)

    session.on("error", _on_error)
    
    ctx.room.on("participant_connected", on_participant_connected)

    ctx.room.on("participant_disconnected", on_participant_disconnected)

    # RoomOptions existed in newer SDKs; older versions only expose RoomInputOptions.
    # Build options in a version-tolerant way.
    room_options = None
    try:
        if hasattr(room_io, "RoomOptions"):
            room_options = room_io.RoomOptions(close_on_disconnect=False)
        elif hasattr(room_io, "RoomInputOptions"):
            room_options = room_io.RoomInputOptions(close_on_disconnect=False)
    except TypeError:
        # Some SDK versions don't accept close_on_disconnect in options.
        if hasattr(room_io, "RoomOptions"):
            room_options = room_io.RoomOptions()
        elif hasattr(room_io, "RoomInputOptions"):
            room_options = room_io.RoomInputOptions()

    # 10) Стартуем сессию
    logging.info("🚀 Starting AgentSession...")
    if room_options is None:
        await session.start(room=ctx.room, agent=teacher_logic)
    else:
        await session.start(room=ctx.room, agent=teacher_logic, room_options=room_options)

    # Пытаемся найти участника, который уже в комнате (это наш юзер с браузера)
    user_name_for_greeting = "Student"
    
    # Берем список всех участников (кроме самого бота)
    participants = list(ctx.room.remote_participants.values())
    
    if participants:
        # Берем первого попавшегося (обычно он один)
        p = participants[0]
        # Берем имя, которое мы передали в токене (из поля Name на сайте)
        if p.name:
            user_name_for_greeting = p.name
            # Сохраняем имя в логику учителя сразу
            teacher_logic.user_name = p.name
            logging.info(f"🚀 FAST START: Found user '{p.name}' immediately!")

    # 🔥 ПРИНУДИТЕЛЬНЫЙ ГЕНЕРАЦИЯ ПЕРВОГО ОТВЕТА
    # Мы даем ИИ скрытую инструкцию: "Поздоровайся с [Имя]".
    logging.info("🗣️ Invoking initial greeting...")
    
    await session.generate_reply(
        instructions=f"The user '{user_name_for_greeting}' has just joined. Greet them warmly by name in German and offer help with learning German. Use system instruction to proceed with the conversation appropriately."
    )


    logging.info("✅ AgentSession started. Running...")
    await stop_event.wait()
    logging.info("🛑 Stop event received — exiting entrypoint")




if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint))



# async def on_function_call этот метод вызывается фреймворком автоматически сразу после того, как generate_reply возвращает None, 

    # it means that LLM вместо текста вернула специальный объект с запросом на вызов функции.

    # модель возвращает строку JSON. Но код получает словарь, потому что фреймворк LIVEKIT выполняет работу по преобразованию строки в словарь за вас. 

    # Это одна из его ключевых задач — избавить вас от рутинной работы по парсингу и предоставить данные в удобном для Python виде.

    # # Псевдокод того, что делает фреймворк "под капотом"

    # json_string_from_llm = '{"identifier": "0501234567"}'

    # arguments_dict = json.loads(json_string_from_llm)

    # arguments_dict теперь является настоящим Python-словарем: {'identifier': '0501234567'}                 
    # # **arguments: Это синтаксис распаковки словаря. Он берет словарь arguments (например, {'identifier': '0501234567'}) 

    # и превращает его в именованные аргументы при вызове функции. То есть, строка выше эквивалентна: func_result = await tool_func(identifier='0501234567')

    # в коде on_function_call нет ни одной строки, которая бы явно отправляла func_result в LLM. 

    # И это потому, что эту работу тоже берет на себя главный цикл фреймворка AgentSession.          
    # 
    # # После того как on_function_call успешно завершается, фреймворк делает следующий шаг. 

    # Он берет результат func_result, упаковывает его в специальное сообщение с "ролью" function и снова отправляет всю историю диалога + этот результат в LLM.

    # Теперь у LLM есть вся информация для финального ответа. Например:

    # История: "Привет" -> "Привет! Назовитесь" -> "Я Иван"

    # Результат функции: {'id': 1, 'first_name': 'Иван', 'is_existing_client': True}

    # На основе этого LLM сгенерирует осмысленный ответ: 

    # "Добрый день, Иван! Рад вас снова слышать. Чем могу помочь?". 

    # Этот текст будет получен и озвучен на следующей итерации цикла, через generate_reply и say



# async def on_function_call_ended Это "хук" (hook), который срабатывает после того, как on_function_call завершился. 

# Он дает вам доступ не только к имени и аргументам, но и к результату (result) или ошибке (error), если она произошла.  
# 
# # Про ключи (LIVEKIT_API_KEY и SECRET):

# Вы правы, мы не передаем их в entrypoint явно. И не нужно. Библиотека livekit-agents (и многие другие серверные программы) по умолчанию спроектирована так, 

# что она автоматически ищет эти данные в переменных окружения. Когда вы запускаете python agent.py, 

# библиотека сама выполняет невидимый для вас os.getenv("LIVEKIT_API_KEY") и os.getenv("LIVEKIT_API_SECRET") для аутентификации на сервере LiveKit.  # Вот полная последовательность событий, начиная с конца on_user_speech_committed:

# on_user_speech_committed завершается. generate_reply вернул None, сигнализируя о вызове функции.

# Дирижер (фреймворк) видит этот сигнал и вызывает вашего первого музыканта: await on_function_call(...).

# Ваш код в on_function_call выполняется. Он находит функцию, вызывает ее и получает func_result (например, словарь с данными Ивана). 

# Метод on_function_call завершает свою работу.Дирижер (фреймворк) перехватывает управление. Он видит, что on_function_call успешно завершился, и у него на руках есть func_result.

# Вот он, скрытый шаг! Дирижер берет func_result, преобразует его в JSON-строку (если это словарь) и добавляет в историю диалога как новое сообщение со специальной ролью function.

# История диалога теперь выглядит так:

# [

#  {'role': 'user', 'content': 'Здравствуйте, это Иван'},

#  {'role': 'assistant', 'tool_calls': [...]}, // Запрос на вызов get_client_info

#  {'role': 'function', 'name': 'get_client_info', 'content': '{"id": 1, "first_name": "Иван", ...}'} // <-- РЕЗУЛЬТАТ

# ]

# Дирижер (фреймворк) немедленно делает новый автоматический вызов LLM, отправляя ей всю эту обновленную историю.

# LLM получает всю картину: что спросил пользователь, какой инструмент она решила вызвать, и какой результат этот инструмент вернул.

# Теперь у LLM есть все данные, и на этот раз она генерирует текстовый ответ: "Добрый день, Иван! Рад вас снова слышать. Чем могу помочь?".

# Дирижер (фреймворк) получает этот текстовый ответ и понимает, что теперь нужно просто его озвучить. Он вызывает вашего второго музыканта: await self.say(...).



#Коли хтось підключиться до кімнати sales-assistant-room (або як вона у вас називається), сервер LiveKit дасть вашому воркеру завдання: 

# "Виконай entrypoint_fnc (тобто вашу функцію entrypoint) для цієї кімнати". 
