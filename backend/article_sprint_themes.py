"""
Artikel Sprint — theme registry (the "code bank" of themes).

Each theme is filled with 250-300 verified German nouns (der/die/das) for the
article-guessing speed game. `subtopics` drive EXHAUSTIVE generation: the fill
job walks EVERY subtopic so a theme covers its whole vocabulary (e.g. Körper →
external + internal organs, skeleton, muscles, every detail), not a random
sample. subtopic labels are in German (they guide German noun generation).

Synced into bt_3_article_sprint_themes by sync_article_sprint_themes_from_code().
"""
from __future__ import annotations

_DEFAULT_TARGET = 280

# (key, label_de, label_ru, subtopics[])
ARTICLE_SPRINT_THEMES: list[dict] = [
    {
        "key": "haus_wohnen", "label_de": "Haus & Wohnen", "label_ru": "Дом и жильё",
        "subtopics": [
            "Wohnformen (Haus, Wohnung, Villa, Bungalow, Hütte)",
            "Räume (Wohnzimmer, Schlafzimmer, Bad, Flur, Keller, Dachboden, Balkon)",
            "Möbel (Sofa, Tisch, Stuhl, Schrank, Bett, Regal, Kommode)",
            "Türen, Fenster, Wände, Boden, Decke, Treppe",
            "Beleuchtung und Lampen",
            "Heizung, Klima, Lüftung",
            "große Haushaltsgeräte (Waschmaschine, Trockner, Staubsauger)",
            "Dekoration, Textilien (Teppich, Vorhang, Kissen, Decke)",
            "Schlüssel, Schlösser, Klingel, Briefkasten",
            "Sanitär (Toilette, Dusche, Waschbecken, Wasserhahn)",
        ],
    },
    {
        "key": "kueche_geschirr", "label_de": "Küche & Geschirr", "label_ru": "Кухня и посуда",
        "subtopics": [
            "Geschirr (Teller, Tasse, Schüssel, Glas, Kanne)",
            "Besteck (Messer, Gabel, Löffel, Schneebesen, Kelle)",
            "Kochgeschirr (Topf, Pfanne, Deckel, Sieb)",
            "Küchengeräte groß (Herd, Backofen, Kühlschrank, Spülmaschine)",
            "Küchengeräte klein (Toaster, Mixer, Wasserkocher, Mikrowelle)",
            "Küchenmöbel und Arbeitsflächen",
            "Vorratsbehälter, Dosen, Gläser",
            "Backzubehör (Backform, Backblech, Nudelholz)",
            "Reinigung (Spülmittel, Lappen, Bürste, Schwamm)",
        ],
    },
    {
        "key": "essen_trinken", "label_de": "Essen & Trinken", "label_ru": "Еда и напитки",
        "subtopics": [
            "Obst", "Gemüse", "Fleisch und Wurst", "Fisch und Meeresfrüchte",
            "Milchprodukte (Milch, Käse, Joghurt, Butter)",
            "Brot und Backwaren", "Süßigkeiten und Desserts",
            "alkoholfreie Getränke", "alkoholische Getränke",
            "Gewürze und Kräuter", "Grundnahrungsmittel (Mehl, Zucker, Reis, Nudeln, Öl)",
            "Mahlzeiten (Frühstück, Mittagessen, Abendessen, Imbiss)",
        ],
    },
    {
        "key": "koerper_gesundheit", "label_de": "Körper & Gesundheit", "label_ru": "Тело и здоровье",
        "subtopics": [
            "Kopf und Gesicht (Stirn, Auge, Nase, Mund, Ohr, Wange, Kinn, Lippe, Augenbraue, Wimper)",
            "Mund innen (Zahn, Zunge, Gaumen, Kiefer)",
            "Hals und Nacken",
            "Rumpf (Brust, Bauch, Rücken, Schulter, Hüfte, Taille)",
            "Arme (Oberarm, Ellbogen, Unterarm, Handgelenk)",
            "Hand (Finger, Daumen, Handfläche, Nagel)",
            "Beine (Oberschenkel, Knie, Wade, Schienbein)",
            "Fuß (Knöchel, Ferse, Zeh, Sohle)",
            "innere Organe (Herz, Lunge, Leber, Niere, Magen, Darm, Milz, Bauchspeicheldrüse, Blase, Gehirn)",
            "Skelett und Knochen (Schädel, Rippe, Wirbelsäule, Becken, Wirbel)",
            "Muskeln, Sehnen, Bänder, Gelenke",
            "Haut, Haare, Nägel",
            "Blut, Gefäße, Lymphe (Ader, Vene, Arterie)",
            "Nervensystem (Nerv, Rückenmark)",
            "Sinnesorgane und ihre Teile",
        ],
    },
    {
        "key": "kleidung_mode", "label_de": "Kleidung & Mode", "label_ru": "Одежда и мода",
        "subtopics": [
            "Oberteile (Hemd, Bluse, Pullover, T-Shirt, Weste)",
            "Jacken und Mäntel", "Unterteile (Hose, Rock, Jeans, Shorts)",
            "Kleider und Anzüge", "Unterwäsche", "Schuhe", "Strümpfe und Socken",
            "Kopfbedeckungen (Hut, Mütze, Kappe)",
            "Accessoires (Gürtel, Schal, Handschuh, Krawatte, Tasche)",
            "Schmuck (Ring, Kette, Ohrring, Armband)",
            "Stoffe und Materialien", "Nähzubehör (Knopf, Reißverschluss, Naht)",
        ],
    },
    {
        "key": "familie_menschen", "label_de": "Familie & Menschen", "label_ru": "Семья и люди",
        "subtopics": [
            "Kernfamilie (Mutter, Vater, Sohn, Tochter, Bruder, Schwester)",
            "Großfamilie (Großmutter, Großvater, Onkel, Tante, Cousin, Cousine, Neffe, Nichte, Enkel)",
            "angeheiratete Verwandtschaft (Schwiegermutter, Schwager, Stiefvater)",
            "Lebensphasen (Baby, Kleinkind, Kind, Jugendlicher, Erwachsener, Rentner)",
            "Beziehungen (Freund, Partner, Ehepartner, Nachbar, Bekannter)",
            "allgemeine Personenbezeichnungen (Mann, Frau, Person, Mensch, Leute, Paar)",
            "soziale Rollen und Gruppen",
        ],
    },
    {
        "key": "beruf_arbeit", "label_de": "Beruf & Arbeit", "label_ru": "Работа и профессии",
        "subtopics": [
            "Berufe Handwerk (Tischler, Maler, Elektriker, Mechaniker)",
            "Berufe Dienstleistung (Verkäufer, Kellner, Friseur, Koch)",
            "Berufe akademisch (Arzt, Anwalt, Ingenieur, Lehrer, Architekt)",
            "Arbeitsplatz und Büro (Schreibtisch, Computer, Drucker, Akte)",
            "Werkzeuge und Arbeitsmittel",
            "Firma und Struktur (Abteilung, Chef, Kollege, Team)",
            "Gehalt, Vertrag, Bewerbung (Lebenslauf, Zeugnis)",
            "Arbeitszeit, Urlaub, Pause, Schicht",
            "Branchen und Wirtschaftszweige",
        ],
    },
    {
        "key": "schule_bildung", "label_de": "Schule & Bildung", "label_ru": "Школа и образование",
        "subtopics": [
            "Schulfächer", "Schulsachen (Heft, Stift, Buch, Tafel, Ranzen, Lineal)",
            "Schulgebäude und Räume (Klassenzimmer, Aula, Turnhalle, Pause)",
            "Personen (Schüler, Lehrer, Direktor, Student)",
            "Prüfungen und Noten (Test, Klausur, Zeugnis, Note)",
            "Bildungsstufen (Kindergarten, Grundschule, Gymnasium, Universität)",
            "Studium (Vorlesung, Seminar, Diplom, Bibliothek, Mensa)",
            "Lernbegriffe (Hausaufgabe, Aufgabe, Lösung, Wissen)",
        ],
    },
    {
        "key": "stadt_gebaeude", "label_de": "Stadt & Gebäude", "label_ru": "Город и здания",
        "subtopics": [
            "öffentliche Gebäude (Rathaus, Kirche, Museum, Bahnhof, Krankenhaus, Bibliothek)",
            "Geschäfte und Läden (Bäckerei, Apotheke, Supermarkt, Kiosk)",
            "Straßen, Plätze, Wege (Gasse, Allee, Kreuzung, Gehweg)",
            "Stadtmöbel (Bank, Laterne, Ampel, Brunnen, Mülleimer)",
            "Infrastruktur (Brücke, Tunnel, Kanal, Mauer)",
            "öffentliche Orte (Park, Markt, Friedhof, Spielplatz)",
            "Wahrzeichen und Türme",
        ],
    },
    {
        "key": "verkehr_reisen", "label_de": "Verkehr & Reisen", "label_ru": "Транспорт и путешествия",
        "subtopics": [
            "Landfahrzeuge (Auto, Bus, Zug, Fahrrad, Motorrad, LKW, Straßenbahn)",
            "Fahrzeugteile (Rad, Motor, Lenkrad, Bremse, Reifen)",
            "Luftfahrt (Flugzeug, Hubschrauber, Rakete)",
            "Wasserfahrzeuge (Schiff, Boot, Fähre, Yacht)",
            "Verkehrsinfrastruktur (Bahnhof, Flughafen, Hafen, Haltestelle, Tankstelle)",
            "Reisegepäck und Dokumente (Koffer, Rucksack, Pass, Ticket, Visum)",
            "Unterkunft (Hotel, Hostel, Zimmer, Rezeption)",
            "Verkehrszeichen und Regeln",
        ],
    },
    {
        "key": "natur_landschaft", "label_de": "Natur & Landschaft", "label_ru": "Природа и ландшафт",
        "subtopics": [
            "Landschaftsformen (Berg, Tal, Hügel, Ebene, Wüste, Insel, Küste, Höhle)",
            "Gewässer (Meer, See, Fluss, Bach, Teich, Quelle, Wasserfall, Welle)",
            "Wald und Vegetation allgemein",
            "Himmelskörper (Sonne, Mond, Stern, Planet, Komet)",
            "Erde, Boden, Gestein (Stein, Fels, Sand, Erde, Lehm)",
            "Naturphänomene (Vulkan, Erdbeben, Lawine)",
            "Himmelsrichtungen und Orientierung",
        ],
    },
    {
        "key": "wetter_jahreszeiten", "label_de": "Wetter & Jahreszeiten", "label_ru": "Погода и сезоны",
        "subtopics": [
            "Niederschlag (Regen, Schnee, Hagel, Tau, Reif)",
            "Wind und Sturm (Wind, Sturm, Brise, Orkan)",
            "Himmel und Wolken (Wolke, Nebel, Gewitter, Blitz, Donner, Regenbogen)",
            "Temperatur und Zustände (Hitze, Kälte, Frost, Wärme)",
            "Jahreszeiten und Monate", "Tageszeiten",
            "Klima und Messung (Thermometer, Barometer, Grad)",
        ],
    },
    {
        "key": "tiere", "label_de": "Tiere", "label_ru": "Животные",
        "subtopics": [
            "Haustiere", "Nutztiere und Bauernhof",
            "Wildtiere Wald", "Wildtiere Savanne und Exoten",
            "Vögel", "Fische und Meerestiere", "Insekten und Spinnen",
            "Reptilien und Amphibien",
            "Tierkörperteile (Schwanz, Flügel, Pfote, Schnabel, Fell, Horn, Kralle)",
            "Tierbehausungen (Nest, Stall, Höhle, Käfig)",
        ],
    },
    {
        "key": "pflanzen_garten", "label_de": "Pflanzen & Garten", "label_ru": "Растения и сад",
        "subtopics": [
            "Bäume (Arten: Eiche, Birke, Tanne, Ahorn)",
            "Blumen", "Sträucher und Büsche",
            "Pflanzenteile (Wurzel, Stamm, Ast, Zweig, Blatt, Blüte, Frucht, Samen, Knospe)",
            "Gartenpflanzen und Kräuter", "Pilze",
            "Gartengeräte (Schaufel, Harke, Gießkanne, Schere, Schubkarre)",
            "Gartenelemente (Beet, Zaun, Gewächshaus, Hecke, Rasen)",
        ],
    },
    {
        "key": "technik_computer", "label_de": "Technik & Computer", "label_ru": "Техника и компьютеры",
        "subtopics": [
            "Computer-Hardware (Bildschirm, Tastatur, Maus, Festplatte, Prozessor, Speicher)",
            "Geräte (Laptop, Tablet, Smartphone, Drucker, Scanner, Router)",
            "Software-Begriffe (Programm, Datei, Ordner, App, System)",
            "Internet und Netzwerk (Verbindung, Netzwerk, Server, Browser)",
            "Elektronik-Bauteile (Chip, Kabel, Stecker, Akku, Schalter)",
            "Strom und Energie (Steckdose, Batterie, Spannung)",
            "Speichermedien (USB-Stick, Karte, Platte)",
        ],
    },
    {
        "key": "medien_kommunikation", "label_de": "Medien & Kommunikation", "label_ru": "Медиа и связь",
        "subtopics": [
            "Printmedien (Zeitung, Zeitschrift, Buch, Artikel, Seite)",
            "Rundfunk (Fernsehen, Radio, Sender, Sendung, Nachricht)",
            "Telefon und Mobilfunk (Anruf, Nummer, Nachricht)",
            "Internet und soziale Medien (Netz, Beitrag, Konto, Profil)",
            "Post und Brief (Brief, Paket, Marke, Umschlag)",
            "Journalismus und Werbung (Bericht, Interview, Anzeige, Werbung)",
            "Film und Video",
        ],
    },
    {
        "key": "sport_freizeit", "label_de": "Sport & Freizeit", "label_ru": "Спорт и досуг",
        "subtopics": [
            "Ballsportarten (Fußball, Tennis, Basketball, Volleyball)",
            "Individualsport (Schwimmen, Laufen, Ski, Boxen, Turnen)",
            "Sportgeräte und Ausrüstung (Ball, Schläger, Netz, Helm, Tor)",
            "Sportorte (Stadion, Halle, Platz, Bahn, Schwimmbad)",
            "Spiele und Brettspiele (Schach, Würfel, Karte, Puzzle)",
            "Hobbys und Freizeit", "Outdoor (Wandern, Camping, Angeln, Zelt)",
            "Wettkampf (Sieg, Niederlage, Pokal, Mannschaft, Spiel, Wettkampf)",
        ],
    },
    {
        "key": "kunst_kultur", "label_de": "Kunst & Kultur", "label_ru": "Искусство и культура",
        "subtopics": [
            "bildende Kunst (Gemälde, Bild, Skulptur, Zeichnung, Pinsel, Farbe)",
            "Musik (Instrumente: Klavier, Gitarre, Geige, Trommel, Flöte)",
            "Musikbegriffe (Lied, Note, Melodie, Chor, Konzert)",
            "Theater und Tanz (Bühne, Schauspieler, Vorhang, Tanz)",
            "Literatur (Roman, Gedicht, Geschichte, Autor)",
            "Film und Kino", "Kulturorte (Museum, Galerie, Oper, Theater)",
        ],
    },
    {
        "key": "wirtschaft_geld", "label_de": "Wirtschaft & Geld", "label_ru": "Экономика и деньги",
        "subtopics": [
            "Geld und Währung (Münze, Schein, Euro, Cent, Geld)",
            "Bank und Konto (Konto, Karte, Automat, Zins)",
            "Bezahlen (Rechnung, Kasse, Bon, Preis, Rabatt)",
            "Handel und Markt (Angebot, Nachfrage, Ware, Kunde, Verkauf)",
            "Wirtschaftsbegriffe (Gewinn, Verlust, Umsatz, Kosten)",
            "Steuern und Versicherung", "Kredit und Schulden",
            "Unternehmen und Börse (Firma, Aktie, Markt)",
        ],
    },
    {
        "key": "gefuehle_charakter", "label_de": "Gefühle & Charakter", "label_ru": "Чувства и характер",
        "subtopics": [
            "positive Gefühle (Freude, Liebe, Glück, Hoffnung, Stolz, Mut)",
            "negative Gefühle (Angst, Wut, Trauer, Neid, Scham, Sorge)",
            "Charaktereigenschaften als Nomen (Ehrlichkeit, Geduld, Faulheit, Fleiß, Geiz)",
            "mentale Zustände (Stress, Ruhe, Müdigkeit, Langeweile)",
            "soziale Gefühle (Vertrauen, Respekt, Mitleid, Eifersucht)",
            "abstrakte Nomen des Geistes (Gedanke, Wille, Erinnerung, Traum)",
        ],
    },
    {
        "key": "medizin", "label_de": "Medizin", "label_ru": "Медицина",
        "subtopics": [
            "Krankheiten und Erkrankungen (Grippe, Krebs, Diabetes, Allergie, Entzündung)",
            "Symptome und Beschwerden (Fieber, Schmerz, Husten, Schwindel, Übelkeit, Ausschlag)",
            "Verletzungen (Wunde, Bruch, Prellung, Verstauchung, Narbe)",
            "medizinische Geräte und Instrumente (Spritze, Skalpell, Stethoskop, Röntgengerät, Ultraschall, Pinzette)",
            "Diagnostik und Untersuchung (Diagnose, Befund, Probe, Messung)",
            "Behandlung und Therapie (Operation, Impfung, Therapie, Behandlung)",
            "Medikamente und Arzneiformen (Tablette, Salbe, Tropfen, Pille, Kapsel, Spritze)",
            "medizinisches Personal und Fachärzte (Arzt, Chirurg, Krankenschwester, Zahnarzt, Apotheker)",
            "Krankenhaus-Abteilungen und -Räume (Station, Notaufnahme, Praxis, Labor)",
            "Hilfsmittel und Verbandsmaterial (Verband, Pflaster, Gips, Krücke, Rollstuhl, Brille)",
            "Erste Hilfe und Notfall (Notruf, Krankenwagen, Verband)",
            "Anatomie-Fachbegriffe und Körpersysteme",
        ],
    },
]


def article_sprint_themes() -> list[dict]:
    """Normalized theme rows for syncing (key, label_de, label_ru, target_count, subtopics)."""
    out: list[dict] = []
    for t in ARTICLE_SPRINT_THEMES:
        key = str(t.get("key") or "").strip()
        if not key:
            continue
        out.append({
            "key": key,
            "label_de": str(t.get("label_de") or "").strip(),
            "label_ru": str(t.get("label_ru") or "").strip(),
            "target_count": int(t.get("target_count") or _DEFAULT_TARGET),
            "subtopics": [str(s).strip() for s in (t.get("subtopics") or []) if str(s).strip()],
        })
    return out
