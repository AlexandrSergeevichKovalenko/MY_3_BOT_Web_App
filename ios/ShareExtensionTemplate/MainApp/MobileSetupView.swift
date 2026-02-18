import SwiftUI
import UserNotifications

struct MobileSetupView: View {
    @Environment(\.openURL) private var openURL
    @State private var baseURL: String = CredentialStore.apiBaseURL?.absoluteString ?? ""
    @State private var accessToken: String = CredentialStore.accessToken ?? ""
    @State private var status: String = ""
    @State private var sampleWord: String = "Haus"
    @State private var dashboard = MobileDashboardResponse.empty
    @State private var dashboardStatus: String = ""
    @State private var notificationsEnabled: Bool = false

    private let api = MobileDictionaryAPI()

    var body: some View {
        NavigationView {
            Form {
                Section(header: Text("Сегодня")) {
                    HStack {
                        Text("На повтор")
                        Spacer()
                        Text("\(dashboard.queueInfo.dueCount)")
                            .fontWeight(.bold)
                    }
                    HStack {
                        Text("Новых на сегодня")
                        Spacer()
                        Text("\(dashboard.queueInfo.newRemainingToday)")
                            .fontWeight(.bold)
                    }
                    if let word = dashboard.wordOfDay?.source, !word.isEmpty {
                        VStack(alignment: .leading, spacing: 4) {
                            Text("Слово дня")
                                .font(.footnote)
                                .foregroundColor(.secondary)
                            Text(word)
                                .fontWeight(.semibold)
                            if let target = dashboard.wordOfDay?.target, !target.isEmpty {
                                Text(target)
                                    .font(.footnote)
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                    Button("Обновить дашборд") {
                        Task { await refreshDashboard() }
                    }
                    if let deeplink = dashboard.deepLinks.review, !deeplink.isEmpty {
                        Button("Открыть повтор в Telegram Mini App") {
                            if let url = URL(string: deeplink) {
                                openURL(url)
                            } else {
                                dashboardStatus = "Некорректный deep link"
                            }
                        }
                    }
                    Toggle("Ежедневное напоминание (19:30)", isOn: $notificationsEnabled)
                        .onChange(of: notificationsEnabled) { value in
                            Task { await updateReminder(enabled: value) }
                        }
                    if !dashboardStatus.isEmpty {
                        Text(dashboardStatus)
                            .font(.footnote)
                            .foregroundColor(.secondary)
                    }
                }

                Section(header: Text("Backend")) {
                    TextField("https://your-backend.example.com", text: $baseURL)
                        .keyboardType(.URL)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                }

                Section(header: Text("Token"), footer: Text("Скопируй из бота командой /mobile_token")) {
                    TextEditor(text: $accessToken)
                        .frame(minHeight: 110)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                }

                Section {
                    Button("Сохранить настройки") {
                        saveSettings()
                    }
                }

                Section(header: Text("Проверка")) {
                    TextField("Тестовое слово", text: $sampleWord)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled(true)
                    Button("Проверить перевод") {
                        Task { await runSmokeTest() }
                    }
                }

                if !status.isEmpty {
                    Section(header: Text("Статус")) {
                        Text(status)
                            .font(.footnote)
                    }
                }
            }
            .navigationTitle("Mobile Setup")
            .task {
                await refreshDashboard()
                notificationsEnabled = await NotificationManager.shared.hasDailyReminder()
            }
        }
    }

    private func saveSettings() {
        let cleanedURL = baseURL.trimmingCharacters(in: .whitespacesAndNewlines)
        let cleanedToken = accessToken.trimmingCharacters(in: .whitespacesAndNewlines)

        guard let url = URL(string: cleanedURL), !cleanedURL.isEmpty else {
            status = "Некорректный URL"
            return
        }
        guard !cleanedToken.isEmpty else {
            status = "Токен пустой"
            return
        }

        CredentialStore.apiBaseURL = url
        CredentialStore.accessToken = cleanedToken
        status = "Сохранено"
    }

    @MainActor
    private func runSmokeTest() async {
        status = "Проверка..."
        do {
            let response = try await api.lookup(word: sampleWord.trimmingCharacters(in: .whitespacesAndNewlines))
            let translation = response.direction == "ru-de" ? (response.item.translationDe ?? "—") : (response.item.translationRu ?? "—")
            status = "OK: \(translation)"
        } catch {
            status = "Ошибка: \(error.localizedDescription)"
        }
    }

    @MainActor
    private func refreshDashboard() async {
        dashboardStatus = "Обновляем..."
        do {
            let data = try await api.dashboard()
            dashboard = data
            dashboardStatus = "OK"
        } catch {
            dashboardStatus = "Ошибка дашборда: \(error.localizedDescription)"
        }
    }

    @MainActor
    private func updateReminder(enabled: Bool) async {
        do {
            if enabled {
                try await NotificationManager.shared.requestAndScheduleDailyReminder(
                    title: "Повтор слов",
                    body: "Открой Mini App и сделай 5 минут повтора."
                )
                dashboardStatus = "Напоминание включено"
            } else {
                NotificationManager.shared.removeDailyReminder()
                dashboardStatus = "Напоминание выключено"
            }
        } catch {
            notificationsEnabled = false
            dashboardStatus = "Уведомления недоступны: \(error.localizedDescription)"
        }
    }
}

#Preview {
    MobileSetupView()
}

private struct MobileDashboardResponse: Decodable {
    struct QueueInfo: Decodable {
        let dueCount: Int
        let newRemainingToday: Int

        enum CodingKeys: String, CodingKey {
            case dueCount = "due_count"
            case newRemainingToday = "new_remaining_today"
        }
    }

    struct WordOfDay: Decodable {
        let entryID: Int?
        let source: String?
        let target: String?
        let createdAt: String?

        enum CodingKeys: String, CodingKey {
            case entryID = "entry_id"
            case source
            case target
            case createdAt = "created_at"
        }
    }

    struct DeepLinks: Decodable {
        let review: String?
        let webapp: String?
    }

    let ok: Bool
    let queueInfo: QueueInfo
    let wordOfDay: WordOfDay?
    let deepLinks: DeepLinks

    enum CodingKeys: String, CodingKey {
        case ok
        case queueInfo = "queue_info"
        case wordOfDay = "word_of_day"
        case deepLinks = "deep_links"
    }

    static let empty = MobileDashboardResponse(
        ok: false,
        queueInfo: QueueInfo(dueCount: 0, newRemainingToday: 0),
        wordOfDay: nil,
        deepLinks: DeepLinks(review: nil, webapp: nil)
    )
}

private extension MobileDictionaryAPI {
    func dashboard() async throws -> MobileDashboardResponse {
        guard let baseURL = CredentialStore.apiBaseURL else {
            throw MobileDictionaryAPIError.missingBaseURL
        }
        guard let token = CredentialStore.accessToken, !token.isEmpty else {
            throw MobileDictionaryAPIError.missingToken
        }
        let url = baseURL.appendingPathComponent("api/mobile/dashboard")
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")

        let (data, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw MobileDictionaryAPIError.badResponse
        }
        if (200...299).contains(http.statusCode) {
            return try JSONDecoder().decode(MobileDashboardResponse.self, from: data)
        }
        if let serverMessage = try? JSONDecoder().decode([String: String].self, from: data)["error"] {
            throw MobileDictionaryAPIError.server(message: serverMessage)
        }
        throw MobileDictionaryAPIError.server(message: "HTTP \(http.statusCode)")
    }
}

private actor NotificationManager {
    static let shared = NotificationManager()
    private let reminderId = "daily-review-reminder"

    func requestAndScheduleDailyReminder(title: String, body: String) async throws {
        let center = UNUserNotificationCenter.current()
        let granted = try await requestAuthorization(center: center)
        guard granted else {
            throw NSError(domain: "NotificationManager", code: 1, userInfo: [NSLocalizedDescriptionKey: "Пользователь не выдал доступ к уведомлениям"])
        }
        removeDailyReminder()

        let content = UNMutableNotificationContent()
        content.title = title
        content.body = body
        content.sound = .default

        var date = DateComponents()
        date.hour = 19
        date.minute = 30
        let trigger = UNCalendarNotificationTrigger(dateMatching: date, repeats: true)
        let req = UNNotificationRequest(identifier: reminderId, content: content, trigger: trigger)
        try await addRequest(center: center, request: req)
    }

    func removeDailyReminder() {
        UNUserNotificationCenter.current().removePendingNotificationRequests(withIdentifiers: [reminderId])
    }

    func hasDailyReminder() async -> Bool {
        let requests = await getPendingRequests(center: UNUserNotificationCenter.current())
        return requests.contains(where: { $0.identifier == reminderId })
    }

    private func requestAuthorization(center: UNUserNotificationCenter) async throws -> Bool {
        try await withCheckedThrowingContinuation { continuation in
            center.requestAuthorization(options: [.alert, .sound, .badge]) { granted, error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                continuation.resume(returning: granted)
            }
        }
    }

    private func addRequest(center: UNUserNotificationCenter, request: UNNotificationRequest) async throws {
        try await withCheckedThrowingContinuation { continuation in
            center.add(request) { error in
                if let error {
                    continuation.resume(throwing: error)
                    return
                }
                continuation.resume(returning: ())
            }
        }
    }

    private func getPendingRequests(center: UNUserNotificationCenter) async -> [UNNotificationRequest] {
        await withCheckedContinuation { continuation in
            center.getPendingNotificationRequests { requests in
                continuation.resume(returning: requests)
            }
        }
    }
}
