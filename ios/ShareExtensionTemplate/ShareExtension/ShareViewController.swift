import UIKit
import UniformTypeIdentifiers

final class ShareViewController: UIViewController {
    private let api = MobileDictionaryAPI()

    private let statusLabel = UILabel()
    private let wordField = UITextField()
    private let translateButton = UIButton(type: .system)
    private let saveButton = UIButton(type: .system)
    private let resultView = UITextView()

    private var currentItem: DictionaryItem?

    override func viewDidLoad() {
        super.viewDidLoad()
        setupUI()
        prefillSharedTextIfAvailable()
    }

    private func setupUI() {
        view.backgroundColor = .systemBackground

        statusLabel.text = "Выделите слово и нажмите Перевести"
        statusLabel.numberOfLines = 0
        statusLabel.font = .preferredFont(forTextStyle: .footnote)

        wordField.placeholder = "Слово на RU или DE"
        wordField.borderStyle = .roundedRect
        wordField.autocorrectionType = .no

        translateButton.setTitle("Перевести", for: .normal)
        translateButton.addTarget(self, action: #selector(onTranslate), for: .touchUpInside)

        saveButton.setTitle("Сохранить", for: .normal)
        saveButton.isEnabled = false
        saveButton.addTarget(self, action: #selector(onSave), for: .touchUpInside)

        resultView.isEditable = false
        resultView.font = .monospacedSystemFont(ofSize: 14, weight: .regular)

        let buttons = UIStackView(arrangedSubviews: [translateButton, saveButton])
        buttons.axis = .horizontal
        buttons.spacing = 12
        buttons.distribution = .fillEqually

        let stack = UIStackView(arrangedSubviews: [statusLabel, wordField, buttons, resultView])
        stack.axis = .vertical
        stack.spacing = 12
        stack.translatesAutoresizingMaskIntoConstraints = false

        view.addSubview(stack)
        NSLayoutConstraint.activate([
            stack.topAnchor.constraint(equalTo: view.safeAreaLayoutGuide.topAnchor, constant: 12),
            stack.leadingAnchor.constraint(equalTo: view.leadingAnchor, constant: 12),
            stack.trailingAnchor.constraint(equalTo: view.trailingAnchor, constant: -12),
            stack.bottomAnchor.constraint(equalTo: view.safeAreaLayoutGuide.bottomAnchor, constant: -12),
            resultView.heightAnchor.constraint(greaterThanOrEqualToConstant: 180)
        ])

        navigationItem.rightBarButtonItem = UIBarButtonItem(
            barButtonSystemItem: .done,
            target: self,
            action: #selector(onDone)
        )
    }

    private func prefillSharedTextIfAvailable() {
        guard let item = extensionContext?.inputItems.first as? NSExtensionItem,
              let providers = item.attachments,
              !providers.isEmpty else {
            return
        }

        for provider in providers {
            if provider.hasItemConformingToTypeIdentifier(UTType.text.identifier) {
                provider.loadItem(forTypeIdentifier: UTType.text.identifier, options: nil) { [weak self] value, _ in
                    guard let self else { return }
                    let raw = (value as? String)?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                    guard !raw.isEmpty else { return }
                    DispatchQueue.main.async {
                        self.wordField.text = String(raw.prefix(64))
                    }
                }
                break
            }
        }
    }

    @objc private func onTranslate() {
        let word = (wordField.text ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !word.isEmpty else {
            statusLabel.text = "Введите или выделите слово."
            return
        }

        saveButton.isEnabled = false
        currentItem = nil
        statusLabel.text = "Перевожу..."

        Task {
            do {
                let response = try await api.lookup(word: word)
                await MainActor.run {
                    self.currentItem = response.item
                    self.resultView.text = self.format(item: response.item, direction: response.direction, sourceWord: word)
                    self.saveButton.isEnabled = true
                    self.statusLabel.text = "Готово"
                }
            } catch {
                await MainActor.run {
                    self.statusLabel.text = error.localizedDescription
                }
            }
        }
    }

    @objc private func onSave() {
        guard let item = currentItem else { return }
        saveButton.isEnabled = false
        statusLabel.text = "Сохраняю..."

        Task {
            do {
                try await api.save(item: item)
                await MainActor.run {
                    self.statusLabel.text = "Сохранено в словарь"
                    self.saveButton.isEnabled = true
                }
            } catch {
                await MainActor.run {
                    self.statusLabel.text = error.localizedDescription
                    self.saveButton.isEnabled = true
                }
            }
        }
    }

    @objc private func onDone() {
        extensionContext?.completeRequest(returningItems: nil, completionHandler: nil)
    }

    private func format(item: DictionaryItem, direction: String, sourceWord: String) -> String {
        let translation = direction == "ru-de" ? (item.translationDe ?? "—") : (item.translationRu ?? "—")
        var lines: [String] = []
        lines.append("Слово: \(sourceWord)")
        lines.append("Направление: \(direction == "ru-de" ? "RU -> DE" : "DE -> RU")")
        lines.append("Перевод: \(translation)")
        lines.append("Часть речи: \(item.partOfSpeech ?? "—")")
        if let article = item.article, !article.isEmpty {
            lines.append("Артикль: \(article)")
        }

        if let forms = item.forms {
            var formLines: [String] = []
            if let v = forms.plural, !v.isEmpty { formLines.append("- Plural: \(v)") }
            if let v = forms.praeteritum, !v.isEmpty { formLines.append("- Prateritum: \(v)") }
            if let v = forms.perfekt, !v.isEmpty { formLines.append("- Perfekt: \(v)") }
            if let v = forms.konjunktiv1, !v.isEmpty { formLines.append("- Konjunktiv I: \(v)") }
            if let v = forms.konjunktiv2, !v.isEmpty { formLines.append("- Konjunktiv II: \(v)") }
            if !formLines.isEmpty {
                lines.append("")
                lines.append("Формы:")
                lines.append(contentsOf: formLines)
            }
        }

        if let examples = item.usageExamples, !examples.isEmpty {
            lines.append("")
            lines.append("Примеры:")
            for (idx, ex) in examples.prefix(3).enumerated() {
                lines.append("\(idx + 1). \(ex)")
            }
        }

        return lines.joined(separator: "\n")
    }
}
