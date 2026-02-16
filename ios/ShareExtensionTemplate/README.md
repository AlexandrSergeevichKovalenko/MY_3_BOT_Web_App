# iOS Share Extension Template (RU/DE Dictionary)

Этот шаблон добавляет в iOS компактный экран перевода из меню `Поделиться`.

## Что умеет
- Берёт выделенный текст из Safari/Chrome/других приложений.
- Делает lookup через `POST /api/mobile/dictionary/lookup`.
- Показывает карточку перевода (перевод, часть речи, артикль, формы, примеры).
- Сохраняет в ту же БД через `POST /api/mobile/dictionary/save`.

## Файлы
- `Shared/DictionaryModels.swift`
- `Shared/CredentialStore.swift`
- `Shared/MobileDictionaryAPI.swift`
- `MainApp/MobileAuthService.swift`
- `MainApp/MobileSetupView.swift`
- `MainApp/TelegramDeutschMobileApp.swift`
- `ShareExtension/ShareViewController.swift`

## Интеграция в Xcode
1. Создай iOS app target (если его нет).
2. Добавь `Share Extension` target (`File -> New -> Target -> Share Extension`).
3. Подключи все Swift-файлы из этого шаблона:
   - `Shared/*` в Main App + Share Extension
   - `MainApp/*` только в Main App
   - `ShareExtension/ShareViewController.swift` только в Share Extension
4. Включи `App Groups` capability в обоих target'ах и поставь один и тот же ID.
5. Замени `CredentialStore.appGroupId` на твой реальный `group.*`.
6. Для быстрого запуска используй готовый экран `MobileSetupView.swift`.

## Быстрый путь (без initData)
1. В личке бота выполни `/mobile_token`.
2. Скопируй `base_url` и `access_token`.
3. Открой iOS Main App и вставь значения в `Mobile Setup`.
4. Нажми `Сохранить настройки`.

## Как пользователь работает
1. Открывает приложение один раз и вставляет backend URL + token из бота.
2. В браузере выделяет слово -> `Поделиться` -> выбирает твой extension.
3. Нажимает `Перевести` -> `Сохранить`.

## Важно
- `Share Extension` не является Telegram WebApp и не открывается поверх сайта как системный floating window.
- На iOS это компактный экран в Share Sheet/Action flow.
