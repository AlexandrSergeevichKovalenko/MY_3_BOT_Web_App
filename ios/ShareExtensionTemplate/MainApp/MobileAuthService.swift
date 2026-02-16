import Foundation

final class MobileAuthService {
    struct ExchangeResponse: Decodable {
        let ok: Bool
        let accessToken: String
        let expiresIn: Int

        enum CodingKeys: String, CodingKey {
            case ok
            case accessToken = "access_token"
            case expiresIn = "expires_in"
        }
    }

    func exchange(initData: String, apiBaseURL: URL) async throws -> ExchangeResponse {
        let url = apiBaseURL.appendingPathComponent("api/mobile/auth/exchange")
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.httpBody = try JSONSerialization.data(withJSONObject: ["initData": initData], options: [])

        let (data, response) = try await URLSession.shared.data(for: req)
        guard let http = response as? HTTPURLResponse else {
            throw URLError(.badServerResponse)
        }
        guard (200...299).contains(http.statusCode) else {
            let server = (try? JSONDecoder().decode([String: String].self, from: data))?["error"]
            throw NSError(domain: "MobileAuthService", code: http.statusCode, userInfo: [NSLocalizedDescriptionKey: server ?? "HTTP \(http.statusCode)"])
        }

        let exchange = try JSONDecoder().decode(ExchangeResponse.self, from: data)
        CredentialStore.apiBaseURL = apiBaseURL
        CredentialStore.accessToken = exchange.accessToken
        return exchange
    }
}
