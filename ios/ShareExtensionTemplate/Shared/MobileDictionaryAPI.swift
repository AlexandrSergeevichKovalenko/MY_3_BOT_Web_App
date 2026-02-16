import Foundation

enum MobileDictionaryAPIError: LocalizedError {
    case missingBaseURL
    case missingToken
    case badResponse
    case server(message: String)

    var errorDescription: String? {
        switch self {
        case .missingBaseURL:
            return "Не настроен API base URL."
        case .missingToken:
            return "Не найден mobile access token. Авторизуйтесь в основном приложении."
        case .badResponse:
            return "Некорректный ответ сервера."
        case .server(let message):
            return message
        }
    }
}

final class MobileDictionaryAPI {
    private let session: URLSession

    init(session: URLSession = .shared) {
        self.session = session
    }

    func lookup(word: String) async throws -> DictionaryLookupResponse {
        try await request(
            path: "api/mobile/dictionary/lookup",
            body: ["word": word],
            decode: DictionaryLookupResponse.self
        )
    }

    func save(item: DictionaryItem) async throws {
        var payload: [String: Any] = [
            "response_json": try item.toDictionary()
        ]
        if let wordRu = item.wordRu, !wordRu.isEmpty { payload["word_ru"] = wordRu }
        if let wordDe = item.wordDe, !wordDe.isEmpty { payload["word_de"] = wordDe }
        if let tDe = item.translationDe, !tDe.isEmpty { payload["translation_de"] = tDe }
        if let tRu = item.translationRu, !tRu.isEmpty { payload["translation_ru"] = tRu }

        _ = try await request(
            path: "api/mobile/dictionary/save",
            body: payload,
            decode: DictionarySaveResponse.self
        )
    }

    private func request<T: Decodable>(path: String, body: [String: Any], decode: T.Type) async throws -> T {
        guard let baseURL = CredentialStore.apiBaseURL else {
            throw MobileDictionaryAPIError.missingBaseURL
        }
        guard let token = CredentialStore.accessToken, !token.isEmpty else {
            throw MobileDictionaryAPIError.missingToken
        }

        let url = baseURL.appendingPathComponent(path)
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        req.httpBody = try JSONSerialization.data(withJSONObject: body, options: [])

        let (data, response) = try await session.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw MobileDictionaryAPIError.badResponse
        }

        if (200...299).contains(http.statusCode) {
            return try JSONDecoder().decode(T.self, from: data)
        }

        if let serverMessage = try? JSONDecoder().decode([String: String].self, from: data)["error"] {
            throw MobileDictionaryAPIError.server(message: serverMessage)
        }

        throw MobileDictionaryAPIError.server(message: "HTTP \(http.statusCode)")
    }
}

private extension Encodable {
    func toDictionary() throws -> [String: Any] {
        let data = try JSONEncoder().encode(self)
        let object = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dict = object as? [String: Any] else {
            return [:]
        }
        return dict
    }
}
