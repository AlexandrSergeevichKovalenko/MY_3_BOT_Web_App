import Foundation

struct DictionaryLookupResponse: Decodable {
    let ok: Bool
    let item: DictionaryItem
    let direction: String
}

struct DictionarySaveResponse: Decodable {
    let ok: Bool
}

struct DictionaryItem: Codable {
    let wordRu: String?
    let wordDe: String?
    let partOfSpeech: String?
    let translationDe: String?
    let translationRu: String?
    let article: String?
    let forms: DictionaryForms?
    let prefixes: [DictionaryPrefix]?
    let usageExamples: [String]?

    enum CodingKeys: String, CodingKey {
        case wordRu = "word_ru"
        case wordDe = "word_de"
        case partOfSpeech = "part_of_speech"
        case translationDe = "translation_de"
        case translationRu = "translation_ru"
        case article
        case forms
        case prefixes
        case usageExamples = "usage_examples"
    }
}

struct DictionaryForms: Codable {
    let plural: String?
    let praeteritum: String?
    let perfekt: String?
    let konjunktiv1: String?
    let konjunktiv2: String?
}

struct DictionaryPrefix: Codable {
    let variant: String?
    let translationDe: String?
    let translationRu: String?
    let explanation: String?
    let exampleDe: String?

    enum CodingKeys: String, CodingKey {
        case variant
        case translationDe = "translation_de"
        case translationRu = "translation_ru"
        case explanation
        case exampleDe = "example_de"
    }
}
