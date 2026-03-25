import json
from pathlib import Path

_project_root = "/Users/simonovikis/RiderProjects/EuroleagueStats"
file_path = Path(_project_root) / "streamlit_app" / "translations.json"

with open(file_path, "r", encoding="utf-8") as f:
    tr = json.load(f)

glossary_keys = {
    "gloss_ortg_title": {
        "en": "📈 Offensive Rating (ORtg)",
        "el": "📈 Επιθετική Αξιολόγηση (ORtg)",
        "de": "📈 Offensiv-Rating (ORtg)",
        "es": "📈 Rating Ofensivo (ORtg)"
    },
    "gloss_ortg_desc": {
        "en": "The total number of points a player or team produces per 100 possessions. This isolates efficiency from the pace of play. If a team scores 85 points in a slow 70-possession game, their impressive `121.4 ORtg` reveals their elite offense despite the low total score.",
        "el": "Οι συνολικοί πόντοι που παράγει μια ομάδα ή παίκτης ανά 100 κατοχές. Απομονώνει την αποτελεσματικότητα από τον ρυθμό. Αν μια ομάδα βάλει 85 πόντους σε έναν αργό αγώνα 70 κατοχών, το `121.4 ORtg` αποδεικνύει την επιθετική της υπεροχή.",
        "de": "Die Gesamtzahl der Punkte, die ein Spieler oder Team pro 100 Ballbesitze erzielt. Das isoliert die Effizienz vom Spieltempo.",
        "es": "El número total de puntos que un jugador o equipo produce por cada 100 posesiones. Esto aísla la eficiencia del ritmo de juego."
    },
    "gloss_drtg_title": {
        "en": "🛡 Defensive Rating (DRtg)",
        "el": "🛡 Αμυντική Αξιολόγηση (DRtg)",
        "de": "🛡 Defensiv-Rating (DRtg)",
        "es": "🛡 Rating Defensivo (DRtg)"
    },
    "gloss_drtg_desc": {
        "en": "The total number of points a team allows per 100 opponent possessions. Lower is better. If a defensive lineup has an `85.0 DRtg` over a 5-minute stretch, they are locking down the opponent.",
        "el": "Οι συνολικοί πόντοι που δέχεται μια ομάδα ανά 100 κατοχές του αντιπάλου. Όσο χαμηλότερα τόσο καλύτερα.",
        "de": "Die Gesamtzahl der Punkte, die ein Team pro 100 gegnerische Ballbesitze zulässt. Niedriger ist besser.",
        "es": "El número total de puntos que un equipo permite por cada 100 posesiones del oponente. Menos es mejor."
    },
    "gloss_net_title": {
        "en": "+/- Net Rating (NetRtg)",
        "el": "+/- Καθαρή Αξιολόγηση (NetRtg)",
        "de": "+/- Net Rating (NetRtg)",
        "es": "+/- Rating Neto (NetRtg)"
    },
    "gloss_net_desc": {
        "en": "`Offensive Rating - Defensive Rating`. A `+15.0 Net Rating` means the 5-man lineup outscores their opponents by 15 points per 100 possessions while on the floor. It is the ultimate metric for lineup synergy.",
        "el": "`ORtg - DRtg`. Ένα Net Rating στο `+15.0` σημαίνει ότι η πεντάδα κερδίζει τον αντίπαλο κατά 15 πόντους ανά 100 κατοχές όσο αγωνίζεται.",
        "de": "`ORtg - DRtg`. Ein Net Rating von `+15.0` bedeutet, die Aufstellung erzielt 15 Punkte mehr pro 100 Ballbesitze.",
        "es": "`ORtg - DRtg`. Un Rating Neto de `+15.0` significa que la alineación supera al oponente por 15 puntos por cada 100 posesiones."
    },
    "gloss_ts_title": {
        "en": "🎯 True Shooting Percentage (TS%)",
        "el": "🎯 Πραγματικό Ποσοστό Ευστοχίας (TS%)",
        "de": "🎯 True Shooting Percentage (TS%)",
        "es": "🎯 Porcentaje de Tiro Verdadero (TS%)"
    },
    "gloss_ts_desc": {
        "en": "A holistic shooting efficiency metric that weighs 3-pointers, 2-pointers, and Free Throws into a single percentage. \n`Formula: Points / (2 * (FGA + (0.44 * FTA)))`. Average Euroleague TS% is ~58%.",
        "el": "Ένας απόλυτος δείκτης ευστοχίας που συνδυάζει τρίποντα, δίποντα και βολές. `Τύπος: Points / (2 * (FGA + (0.44 * FTA)))`.",
        "de": "Eine holistische Wurf-Effizienz-Metrik, die 3-Punkt-, 2-Punkt- und Freiwürfe in einen einzigen Prozentsatz wichtet.",
        "es": "Una métrica de eficiencia de tiro holística que pondera tiros de 3, tiros de 2 y tiros libres."
    },
    "gloss_tusg_title": {
        "en": "🔥 True Usage Rate (tUSG%)",
        "el": "🔥 Πραγματικό Ποσοστό Χρήσης (tUSG%)",
        "de": "🔥 True Usage Rate (tUSG%)",
        "es": "🔥 Tasa de Uso Verdadero (tUSG%)"
    },
    "gloss_tusg_desc": {
        "en": "The percentage of team plays \"used\" by a specific player while they are on the floor (ending in a shot, free throw, or turnover). High-volume stars typically have a usage rate over `25%`.",
        "el": "Το ποσοστό των ομαδικών επιθέσεων που \"ξοδεύει\" ένας παίκτης όσο παίζει (σε σουτ, βολές, ή λάθος).",
        "de": "Der Prozentsatz der Spielzüge, die ein bestimmter Spieler auf dem Spielfeld abschließt.",
        "es": "El porcentaje de jugadas terminadas por un jugador en el campo."
    },
    "gloss_stop_title": {
        "en": "🧱 Stop Rate",
        "el": "🧱 Ποσοστό Στοπ (Stop Rate)",
        "de": "🧱 Stop Rate",
        "es": "🧱 Tasa de Paradas (Stop Rate)"
    },
    "gloss_stop_desc": {
        "en": "The percentage of an opponent's possessions that end in a \"Stop\" (Missed shot + Defensive Rebound, or a forced Turnover) while a player or lineup is on the floor.",
        "el": "Το ποσοστό των αντιπάλων επιθέσεων που καταλήγουν σε \"Στοπ\" (χαμένο σουτ + αμυντικό ριμπάουντ, ή λάθος).",
        "de": "Prozentsatz der gegnerischen Ballbesitze, die ohne Punkt enden.",
        "es": "El porcentaje de las posesiones del oponente que terminan sin anotar."
    },
    "gloss_ast_title": {
        "en": "🤝 Assist Ratio (AST%)",
        "el": "🤝 Ποσοστό Ασίστ (AST%)",
        "de": "🤝 Assist Ratio (AST%)",
        "es": "🤝 Ratio de Asistencias (AST%)"
    },
    "gloss_ast_desc": {
        "en": "An estimate of the percentage of teammate field goals a player assisted while on the floor. High-IQ point guards frequently exceed `35%`.",
        "el": "Εκτίμηση του ποσοστού των καλαθιών των συμπαικτών που προήλθαν από ασίστ του παίκτη. Καλoί playmaker φτάνουν το 35%.",
        "de": "Eine Schätzung des Prozentsatzes der von Mitspielern erzielten Field Goals, die assistiert wurden.",
        "es": "Estimación del porcentaje de tiros de campo anotados que el jugador asistió."
    },
    "gloss_tov_title": {
        "en": "🗑 Turnover Ratio (TOV%)",
        "el": "🗑 Ποσοστό Λαθών (TOV%)",
        "de": "🗑 Turnover Ratio (TOV%)",
        "es": "🗑 Ratio de Pérdidas (TOV%)"
    },
    "gloss_tov_desc": {
        "en": "An estimate of turnovers committed per 100 plays. Lower is better.",
        "el": "Εκτίμηση των λαθών ανά 100 κατοχές. Όσο μικρότερο τόσο καλύτερα.",
        "de": "Eine Schätzung der begangenen Turnovers pro 100 Spielzüge.",
        "es": "Una estimación de las pérdidas cometidas por 100 jugadas."
    },
    "gloss_tip_title": {
        "en": "💡 **Why Per 100 Possessions?**",
        "el": "💡 **Γιατί ανά 100 Κατοχές;**",
        "de": "💡 **Warum pro 100 Ballbesitze?**",
        "es": "💡 **¿Por qué por 100 Posesiones?**"
    },
    "gloss_tip_desc": {
        "en": "Basketball is a game of alternating turns. By analyzing stats *Per 100 Possessions*, we completely eliminate pace variance.",
        "el": "Το μπάσκετ είναι παιχνίδι εναλλασσόμενων κατοχών. Μελετώντας στατιστικά *ανά 100 κατοχές*, διαγράφουμε τον ρυθμό και βρίσκουμε την αληθινή αποδοτικότητα.",
        "de": "Basketball ist ein Rhythmus-Spiel. Durch die Analyse pro 100 Ballbesitze eliminieren wir die Tempoabweichungen.",
        "es": "El baloncesto depende del ritmo. Al analizamos por 100 posesiones, eliminamos la variación de la velocidad del juego."
    }
}

tr.update(glossary_keys)
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(tr, f, ensure_ascii=False, indent=2)

print("Glossary keys appended to translations.json successfully.")
