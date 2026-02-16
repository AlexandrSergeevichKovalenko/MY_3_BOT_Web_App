import Foundation

enum CredentialStore {
    // Replace with your real App Group ID in both Main App and Share Extension targets.
    static let appGroupId = "group.com.example.telegramdeutsch"

    private static let baseURLKey = "mobile_api_base_url"
    private static let tokenKey = "mobile_access_token"

    private static var defaults: UserDefaults? {
        UserDefaults(suiteName: appGroupId)
    }

    static var apiBaseURL: URL? {
        get {
            guard let raw = defaults?.string(forKey: baseURLKey)?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !raw.isEmpty else {
                return nil
            }
            return URL(string: raw)
        }
        set {
            defaults?.setValue(newValue?.absoluteString, forKey: baseURLKey)
        }
    }

    static var accessToken: String? {
        get {
            defaults?.string(forKey: tokenKey)?.trimmingCharacters(in: .whitespacesAndNewlines)
        }
        set {
            defaults?.setValue(newValue, forKey: tokenKey)
        }
    }
}
