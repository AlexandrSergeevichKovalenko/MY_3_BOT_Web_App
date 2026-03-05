import os
import json
from uuid import uuid4
import logging
from livekit.agents import llm
import asyncio
from typing import Optional, List, Dict
from database import get_db_connection_context  
from openai_manager import llm_execute
from config_mistakes_data import (
    VALID_CATEGORIES, 
    VALID_SUBCATEGORIES, 
    VALID_CATEGORIES_lower, 
    VALID_SUBCATEGORIES_lower,
)


# Импортируем все необходимые функции из нашего обновленного database.py
# from database import (
#     get_client_by_identifier,
#     create_client,
#     get_new_products,
#     get_product_by_name,
#     record_order,
#     get_manager_contact_by_location
# )

# Файл api.py — это четко определенный интерфейс между возможностями вашего приложения и языковой моделью. 
# Он "обертывает" функции работы с базой данных в "инструменты", которые LLM может понимать и запрашивать для выполнения задач.

# Когда вы помечаете асинхронную функцию @llm.function_tool декоратором, 
# LiveKit Agents автоматически:
# Создает JSON Schema описание этой функции. Это описание включает имя функции, её docstring
# Передает это JSON Schema описание в LLM. Таким образом, LLM "узнаёт", какие функции ей доступны, что они делают и как их вызывать.
# Обрабатывает вызовы LLM. Когда LLM решает вызвать одну из этих функций, 
# LiveKit Agents перехватывает этот вызов и выполняет соответствующий метод in Python-коде.

# пример того как возвращает информацию LLM для вызова функций соответствующей
# {
#   "tool_calls": [
#     {
#       "function": {
#         "name": "get_client_info",
#         "arguments": {
#           "identifier": "0501234567"
#         }
#       }
#     }
#   ]
# }

# риложение (LiveKit Agent) получает этот JSON-объект с вызовом инструмента от LLM.
# Затем фактически вызывает вашу Python-функцию get_client_info (которая находится в классе SalesAssistantTools в api.py) 
# с этими аргументами: await self.tools.get_client_info(identifier="0501234567").
# Результат выполнения Python-функции (например, { "id": 123, "first_name": "Анна", ... }) передается обратно LLM.
# LLM, имея теперь этот результат, формулирует окончательный ответ пользователю на естественном языке, например: "Добрий день, Анно! Радий вас знову чути. Чим можу допомогти?"

# Когда мы просим LLM вызвать функцию, она не "выполняет" 
# код Python напрямую и не "строит" сложные Python-объекты (вроде списка словарей list[dict]) в своей внутренней среде, а затем передает их в нашу функцию.
# LLM генерирует текстовое представление вызова функции. Это текстовое представление должно быть таким, 
# чтобы наша программа на Python могла его легко "прочитать" и "понять", во что это текстовое представление должно быть преобразовано для вызова реальной функции.
# Он делает это в формате JSON, потому что мы ему так сказали в docstring функции, и он знает, что JSON – это универсальный, структурированный текстовый формат.
# LLM не может генерировать list[dict] напрямую!!!
#Потому что LLM – это текстовые модели. Их выход – всегда текст. 
# Когда они генерируют "вызов функции", они фактически генерируют текст, который соответствует заранее определенному JSON-формату вызова. 
# В этом JSON-формате значения аргументов функции также должны быть представлены в виде текста: строки, числа, булевы значения. 
# Для более сложных структур данных, таких как списки словарей, стандартный способ передать их через текстовый интерфейс – это сериализовать их в JSON-строку.

# class SalesAssistantTools:
#     def __init__(self):
#         pass

#     @llm.function_tool
#     async def get_client_info(self, identifier: str) -> Dict:
#         """
#         Retrieves client information by system ID or phone number.
#         Parameters:
#         - identifier: string (required) - The client's system ID or full phone number.
#         Returns: A dictionary with client data (id, first_name, last_name, phone_number, email, location, manager_contact, is_existing_client)
#                  or an empty dictionary if the client is not found.
#         """
#         client_data = await get_client_by_identifier(identifier)
#         return client_data if client_data else {}

#     @llm.function_tool
#     async def create_or_update_client(
#         self,
#         first_name: str,
#         phone_number: str,
#         last_name: Optional[str] = None,
#         system_id: Optional[str] = None,
#         email: Optional[str] = None,
#         location: Optional[str] = None,
#         manager_contact: Optional[str] = None,
#         is_existing_client: Optional[bool] = False
#     ) -> Dict:
#         """
#         Creates a new client record or updates an existing one in the database.
#         Used when the assistant collects all necessary client information.
#         If a client with the given phone number already exists, their data will be updated.
#         Parameters:
#         - first_name: string (required) - The client's first name.
#         - phone_number: string (required) - The client's phone number (must be unique).
#         - last_name: string (optional) - The client's surname.
#         - system_id: string (optional) - The client's unique system ID.
#         - email: string (optional) - The client's email address.
#         - location: string (optional) - The client's city or region.
#         - manager_contact: string (optional) - Contact details of the responsible manager.
#         - is_existing_client: boolean (optional) - True if the client is already working with us, False otherwise.
#         Returns: A dictionary with the created/updated client's data.
#         """
#         if not phone_number:
#             raise ValueError("phone_number must not be empty")
#         return await create_client(
#             first_name, phone_number, last_name, system_id, email, location, manager_contact, is_existing_client
#         )

#     @llm.function_tool
#     async def get_new_products_info(self) -> List[Dict]:
#         """
#         Retrieves a list of all products marked as new.
#         Used when the user asks about new arrivals or company novelties.
#         Returns: A list of dictionaries, each containing 'id', 'name', 'description', 'price' of new products.
#         """
#         return await get_new_products()

#     @llm.function_tool
#     async def get_product_details(self, product_name: str) -> Dict:
#         """
#         Retrieves detailed information about a specific product by its name.
#         Used when the user is interested in a particular product.
#         Parameters:
#         - product_name: string (required) - The name of the product.
#         Returns: A dictionary with product data (id, name, description, price, available_quantity)
#                  or an empty dictionary if the product is not found.
#         """
#         product_data = await get_product_by_name(product_name)
#         return product_data if product_data else {}

#     @llm.function_tool
#     async def record_customer_order(
#         self,
#         client_id: int,
#         products_info: str, #параметр products_info формируется на основе разговора с клиентом. Конкретно, это делает языковая модель (LLM), которая интегрирована через OpenAI API и используется LiveKit Agents.
#         status: str = 'pending'
#     ) -> str:
#         """
#         Records a new order in the database.
#         Used when the user is ready to place a purchase.
#         Important: The LLM must convert the list of products and their quantities into a JSON string for the 'products_info' parameter.
#         Example 'products_info': '[{"product_id": 1, "quantity": 2}, {"product_id": 4, "quantity": 1}]'
#         Parameters:
#         - client_id: integer (required) - The ID of the client placing the order (obtained from get_client_info or create_or_update_client).
#         - products_info: string (required) - A JSON string representing a list of dictionaries, each containing 'product_id' (integer) and 'quantity' (integer).
#         - status: string (optional) - The status of the order (e.g., 'pending', 'completed', 'cancelled'). Defaults to 'pending'.
#         Returns: A JSON string with the recorded order's data.
#         """
#         try:
#             products_with_quantity = json.loads(products_info)
#             if not isinstance(products_with_quantity, list):
#                 raise ValueError("products_info must be a JSON string representing a list.")
#             for item in products_with_quantity:
#                 if not isinstance(item, dict) or "product_id" not in item or "quantity" not in item:
#                     raise ValueError("Each item in products_info must be a dictionary with 'product_id' and 'quantity'.")
#         except json.JSONDecodeError as e: 
#             #Если строка JSON некорректна (например, содержит синтаксические ошибки), возникает json.JSONDecodeError, который логируется, 
#             # и выбрасывается ValueError с описанием проблемы.
#             logging.error(f"Invalid JSON in products_info: {products_info}, error: {e}")
#             raise ValueError(f"products_info must be a valid JSON string, got: {products_info}")
#         order_result = await record_order(client_id, products_with_quantity, status)
#         return json.dumps(order_result) # Преобразуем результат в строку JSON

#     @llm.function_tool
#     async def get_manager_for_location(self, location: str) -> Dict:
#         """
#         Retrieves the contact details of the manager responsible for the specified location.
#         Used when the user asks who their manager is or who is responsible for a specific region.
#         Parameters:
#         - location: string (required) - The client's city or region.
#         Returns: A dictionary with the manager's contact details (e.g., {'contact': 'Name: John Doe, Phone: +123456789'})
#                  or an empty dictionary if no manager is found for the location.
#         """
#         contact = await get_manager_contact_by_location(location)
#         return {"contact": contact} if contact else {}

# ... (після закоментованого коду) ...

class GermanTeacherTools:
    def __init__(self, session_id):
        # Мы принимаем session_id при создании экземпляра класса и сохраняем его
        self.session_id = session_id

        # Ми можемо тут нічого не ініціалізувати,
        # а відкривати з'єднання з БД всередині кожного інструменту.
        logging.info(f"GermanTeacherTools instance created for session: {session_id}")

    @llm.function_tool
    async def get_recent_telegram_mistakes(self, user_id: int) -> Dict:
        """
        Analyzes the user's recent mistakes from the Telegram bot (bt_3_detailed_mistakes table)
        to identify the 1-2 most common error categories (focus topics).
        Also returns 1-2 specific examples of these mistakes.

        Parameters:
        - user_id: int (required) - The unique identifier for the student.

        Returns:
        A dictionary containing:
        - "main_focus_topic": string (e.g., "Cases - Dativ") - The most common error category.
        - "secondary_focus_topic": string (e.g., "Verbs - Perfect Tense") - The second most common error category.
        - "recent_mistake_examples": List[Dict] (e.g., [{"original": "...", "error": "...", "correct": "..."}]) - it can be approx. 4 examples.
        - "status": string ("Found" or "NoMistakesFound") - Status of the search.
        """
        logging.info(f"Tool 'get_recent_telegram_mistakes' called for user_id: {user_id}")
        
        focus_topics = []
        examples = []
        
        try:
            # Використовуємо наш менеджер контексту з database.py для безпечного з'єднання
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    
                    # 1. Знаходимо 2 найчастіші теми помилок (категорія + підкатегорія)
                    # Ми беремо помилки за останні 120 днів (можна змінити)
                    # COUNT(*) — це функція, яка при роботі з GROUP BY підраховує, скільки оригінальних рядків було "схлопнуто" в кожну групу.
                    cursor.execute("""
                        SELECT main_category, sub_category, COUNT(*) as error_count
                        FROM bt_3_detailed_mistakes
                        WHERE user_id = %s AND last_seen >= NOW() - INTERVAL '120 days'
                        GROUP BY main_category, sub_category
                        ORDER BY error_count DESC
                        LIMIT 2;
                    """, (user_id,))
                    
                    top_mistakes = cursor.fetchall()
                    
                    if top_mistakes:
                        for i, (main_cat, sub_cat, count) in enumerate(top_mistakes):
                            topic_name = f"{main_cat} - {sub_cat}"
                            focus_topics.append(topic_name)

                        # 2. Отримуємо 2 приклади для кожної теми помилок
                        # main_focus_main_cat = top_mistakes[0][0]
                        # main_focus_sub_cat = top_mistakes[0][1]
                        
                            cursor.execute("""
                                SELECT sentence, correct_translation, (
                                -- Початок "Внутрішнього" (корельованого) підзапиту
                                    SELECT user_translation 
                                    FROM bt_3_translations t 
                                    WHERE t.id_for_mistake_table = m.sentence_id 
                                    AND t.user_id = m.user_id
                                    ORDER BY t.timestamp DESC 
                                    LIMIT 1
                                -- Кінець "Внутрішнього" підзапиту
                                ) as user_error
                                FROM bt_3_detailed_mistakes m
                                WHERE m.user_id = %s 
                                AND m.main_category = %s 
                                AND m.sub_category = %s
                                ORDER BY m.last_seen DESC
                                LIMIT 2;
                            """, (user_id, main_cat, sub_cat))
                        
                            example_rows = cursor.fetchall()
                            for row in example_rows:
                                examples.append({
                                    "original": row[0],
                                    "error": row[2] or "N/A", # 'user_error'
                                    "correct": row[1],
                                    "category": topic_name # Добавил метку категории для ясности
                                })
            
            # 3. Формуємо фінальний словник
            if not focus_topics:
                logging.info(f"No recent mistakes found for user {user_id}.")
                return {
                    "main_focus_topic": None,
                    "secondary_focus_topic": None,
                    "recent_mistake_examples": [],
                    "status": "NoMistakesFound"
                }

            result = {
                "main_focus_topic": focus_topics[0] if len(focus_topics) > 0 else None,
                "secondary_focus_topic": focus_topics[1] if len(focus_topics) > 1 else None,
                "recent_mistake_examples": examples,
                "status": "Found"
            }
            
            logging.info(f"Returning focus topics for user {user_id}: {result}")
            return result

        except Exception as e:
            logging.error(f"Error in 'get_recent_telegram_mistakes': {e}", exc_info=True)
            # Повертаємо помилку, яку LLM зможе зрозуміти
            return {
                "main_focus_topic": None,
                "secondary_focus_topic": None,
                "recent_mistake_examples": [],
                "status": f"Error: {str(e)}"
            }
        
    

    @llm.function_tool
    async def log_conversation_mistake(
        self, 
        user_id: int, 
        user_sentence: str, 
        correct_sentence: str, 
        main_category: str, 
        sub_category: str,
        explanation: str
    ) -> str:
        """
        Logs a grammatical mistake made by the user DURING the voice conversation.
        
        CRITICAL RULE FOR MULTIPLE MISTAKES:
        If the user makes multiple mistakes in a single sentence, Identify and log ONLY the ONE "Most Significant Error".
        The "Most Significant Error" is the one that most severely impacts meaning or violates a fundamental grammar rule 
        (e.g., wrong verb tense or wrong auxiliary verb is more important than a minor adjective ending).

        STRICT RULES FOR CATEGORIES:
        You MUST select the `main_category` and `sub_category` STRICTLY from the lists below.
        
        Valid Categories and Subcategories:
        - Use the CURRENT runtime taxonomy from `config_mistakes_data`
          (`VALID_CATEGORIES` + `VALID_SUBCATEGORIES`).
        - `sub_category` MUST belong to selected `main_category`.
        - If uncertain, use fallback:
          main_category = "Other mistake", sub_category = "Unclassified mistake".

        Parameters:
        - user_id: int (required)
        - user_sentence: str (required) - The incorrect sentence spoken by the user.
        - correct_sentence: str (required) - The corrected version.
        - main_category: str (required) - One of the valid Main Categories above.
        - sub_category: str (required) - One of the valid Subcategories matching the Main Category.
        - explanation: str (required) - A very short explanation of the error in Russian (e.g., "Глаголы движения требуют sein").

        Returns:
        - str: Confirmation message.
        """
        logging.info(f"Tool 'log_conversation_mistake' called. Input -> Cat: {main_category}, Sub: {sub_category}")

        # 1. Нормалізація (Lower case check)
        main_lower = main_category.strip().lower()
        sub_lower = sub_category.strip().lower()

        final_main_cat = "Other mistake"
        final_sub_cat = "Unclassified mistake"
        is_valid = False

        # 2. Перевірка валідності через словники (які ми імпортували з bot_3)
        if main_lower in VALID_SUBCATEGORIES_lower:
            if sub_lower in VALID_SUBCATEGORIES_lower[main_lower]:
                is_valid = True
                
                # 3. Відновлення "Красивого" (Original) регістру для запису в БД
                # Знаходимо ключ (Main), який при lower() дає main_lower
                original_main = next((k for k in VALID_SUBCATEGORIES.keys() if k.lower() == main_lower), main_category)
                
                # Знаходимо значення (Sub) у списку цього ключа
                original_sub_list = VALID_SUBCATEGORIES[original_main]
                original_sub = next((s for s in original_sub_list if s.lower() == sub_lower), sub_category)

                final_main_cat = original_main
                final_sub_cat = original_sub
                logging.info(f"✅ Valid category found: {final_main_cat} - {final_sub_cat}")
            else:
                 logging.warning(f"⚠️ Subcategory '{sub_category}' not found in '{main_category}'.")
        else:
             logging.warning(f"⚠️ Main category '{main_category}' is invalid.")

        if not is_valid:
            logging.info("⚠️ Logging as 'Other mistake' due to validation failure.")

        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Записуємо в нову таблицю bt_3_conversation_errors
                    # session_id поки залишаємо NULL, бо ми його не передаємо з агента (це можна додати пізніше)
                    cursor.execute("""
                        INSERT INTO bt_3_conversation_errors 
                        (user_id, session_id, sentence_with_error, corrected_sentence, error_type, error_subtype, explanation_ru, timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
                    """, (user_id, self.session_id, user_sentence, correct_sentence, final_main_cat, final_sub_cat, explanation))
            
            return f"Mistake logged: {final_main_cat} - {final_sub_cat}"

        except Exception as e:
            logging.error(f"Error in 'log_conversation_mistake': {e}", exc_info=True)
            return f"Failed to log mistake: {str(e)}"
        

    @llm.function_tool
    async def explain_grammar(self, topic: str) -> str:
        """
        Explains a German grammar topic using lifehacks, mnemonics, or analogies.
        
        STYLE GUIDELINES:
        - NO boring lectures. NO long explanations.
        - Use "Lifehacks" or "Mnemonics" or "GUT FILLING" how to understand and conceive the essence of the user mistake (e.g., "Dativ is like giving something To someone").
        - Keep it under 40 seconds (approx. 60-80 words).
        - Be charismatic and encouraging.

        Parameters:
        - topic: string (required) - The grammar topic (e.g. "Dativ", "Adjective Endings").

        Returns:
        A string with the explanation.
        """
        logging.info(f"Tool 'explain_grammar' called for topic: {topic}")
        
        try:
            explanation = await llm_execute(
                task_name="explain_grammar_tool",
                system_instruction_key="explain_grammar_tool",
                user_message=f"Topic: {topic}",
                poll_interval_seconds=1.0,
            )
            return str(explanation or "").strip()

        except Exception as e:
            logging.error(f"Error in 'explain_grammar': {e}", exc_info=True)
            return f"Fehler bei der Erklärung: {str(e)}"

    @llm.function_tool
    async def bookmark_phrase(self, user_id: int, phrase: str, context_note: str) -> str:
        """
        Saves a specific German phrase or word to the user's bookmarks for later review.
        Call this when the user says "Save this phrase", "Bookmark this", or "I want to learn this word".

        Parameters:
        - user_id: int (required)
        - phrase: str (required) - The German phrase or word to save.
        - context_note: str (required) - A brief note about meaning or why it was saved (e.g., "Means 'to give up'").

        Returns:
        - str: Confirmation message.
        """
        logging.info(f"Tool 'bookmark_phrase' called. Phrase: {phrase}")

        try:
            with get_db_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Припускаємо, що таблиця називається bt_3_bookmarks
                    # session_id беремо з self.session_id (як ми зробили для помилок)
                    cursor.execute("""
                        INSERT INTO bt_3_bookmarks 
                        (user_id, session_id, phrase, context_note, timestamp)
                        VALUES (%s, %s, %s, %s, NOW());
                    """, (user_id, self.session_id, phrase, context_note))
            
            return f"Phrase saved: '{phrase}'."

        except Exception as e:
            logging.error(f"Error in 'bookmark_phrase': {e}", exc_info=True)
            return f"Failed to save bookmark: {str(e)}"
    

    @llm.function_tool
    async def generate_quiz_question(self, topic: str) -> Dict:
        """
        Generates a single quiz question (multiple-choice or fill-in-the-blank) 
        about a specific German grammar topic, returning it in JSON format.
        """
        logging.info(f"Tool 'generate_quiz_question' called for topic: {topic}")
        
        question_id = int(uuid4().hex[:12], 16)  # Генеруємо унікальний ідентифікатор питання

        try:
            response_text = await llm_execute(
                task_name="generate_quiz_question_tool",
                system_instruction_key="generate_quiz_question_tool",
                user_message=f"Topic: {topic}",
                poll_interval_seconds=1.0,
            )
            quiz_data = json.loads(str(response_text or "{}"))
            quiz_data["question_id"] = question_id
            return quiz_data

        except Exception as e:
            logging.error(f"Error in 'generate_quiz_question': {e}", exc_info=True)
            return {"error": str(e)}
   
    @llm.function_tool
    async def evaluate_quiz_answer(self, question_text: str, correct_answer: str, user_answer: str) -> Dict:
        """
        Evaluates the user's spoken answer to a quiz question.
        """
        logging.info(f"Tool 'evaluate_quiz_answer' called.")
        
        try:
            response_text = await llm_execute(
                task_name="evaluate_quiz_answer_tool",
                system_instruction_key="evaluate_quiz_answer_tool",
                user_message=(
                    f"Q: {question_text}\n"
                    f"Correct: {correct_answer}\n"
                    f"User: {user_answer}"
                ),
                poll_interval_seconds=1.0,
            )
            return json.loads(str(response_text or "{}"))

        except Exception as e:
            logging.error(f"Error in 'evaluate_quiz_answer': {e}", exc_info=True)
            return {"error": str(e)}
