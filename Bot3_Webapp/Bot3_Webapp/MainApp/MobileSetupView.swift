import SwiftUI

struct MobileSetupView: View {
    @State private var baseURL: String = CredentialStore.apiBaseURL?.absoluteString ?? ""
    @State private var accessToken: String = CredentialStore.accessToken ?? ""
    @State private var status: String = ""
    @State private var sampleWord: String = "Haus"

    private let api = MobileDictionaryAPI()

    var body: some View {
        NavigationView {
            Form {
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
}

#Preview {
    MobileSetupView()
}
