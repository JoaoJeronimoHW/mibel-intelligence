"""
Weather locations: the single registry used by weather_ingest (what to
download) and build_panel (how to aggregate to country level).

Every location has a role:
- 'main': the country's largest cities. Their unweighted mean gives the
  country's weather *levels* (temperature_c, wind_speed_100m, ...).
- 'reference': extra cities chosen to cover climate regions and renewable
  hot spots the main cities miss (windy coasts and plains, sunny south,
  mountains). Together with the main cities they give the country's
  intra-country weather *dispersion* (the *_sd and *_range columns).

The ES and PT main cities are the original seven Iberian locations, so the
Iberian level variables are unchanged.
"""

LOCATIONS = {
    # ---- Spain ----
    'Madrid':          {'country': 'ES', 'role': 'main',      'lat': 40.4168, 'lon': -3.7038},   # Central meseta
    'Barcelona':       {'country': 'ES', 'role': 'main',      'lat': 41.3874, 'lon': 2.1686},    # Northeast coast
    'Seville':         {'country': 'ES', 'role': 'main',      'lat': 37.3891, 'lon': -5.9845},   # South (Andalusia)
    'Bilbao':          {'country': 'ES', 'role': 'main',      'lat': 43.2630, 'lon': -2.9350},   # North coast
    'A_Coruna':        {'country': 'ES', 'role': 'reference', 'lat': 43.3623, 'lon': -8.4115},   # Galicia, Atlantic wind
    'Valladolid':      {'country': 'ES', 'role': 'reference', 'lat': 41.6523, 'lon': -4.7245},   # Northern meseta, wind
    'Zaragoza':        {'country': 'ES', 'role': 'reference', 'lat': 41.6488, 'lon': -0.8891},   # Ebro valley (cierzo wind)
    'Valencia':        {'country': 'ES', 'role': 'reference', 'lat': 39.4699, 'lon': -0.3763},   # East Mediterranean coast
    'Almeria':         {'country': 'ES', 'role': 'reference', 'lat': 36.8340, 'lon': -2.4637},   # Southeast, sunniest

    # ---- Portugal ----
    'Lisbon':          {'country': 'PT', 'role': 'main',      'lat': 38.7223, 'lon': -9.1393},   # Centre coast
    'Porto':           {'country': 'PT', 'role': 'main',      'lat': 41.1579, 'lon': -8.6291},   # North coast
    'Faro':            {'country': 'PT', 'role': 'main',      'lat': 37.0194, 'lon': -7.9322},   # South (Algarve)
    'Braganca':        {'country': 'PT', 'role': 'reference', 'lat': 41.8061, 'lon': -6.7567},   # Northeast interior
    'Coimbra':         {'country': 'PT', 'role': 'reference', 'lat': 40.2033, 'lon': -8.4103},   # Centre
    'Castelo_Branco':  {'country': 'PT', 'role': 'reference', 'lat': 39.8222, 'lon': -7.4909},   # Centre interior
    'Evora':           {'country': 'PT', 'role': 'reference', 'lat': 38.5714, 'lon': -7.9135},   # Alentejo, solar

    # ---- France ----
    'Paris':           {'country': 'FR', 'role': 'main',      'lat': 48.8566, 'lon': 2.3522},
    'Lyon':            {'country': 'FR', 'role': 'main',      'lat': 45.7640, 'lon': 4.8357},
    'Marseille':       {'country': 'FR', 'role': 'main',      'lat': 43.2965, 'lon': 5.3698},
    'Toulouse':        {'country': 'FR', 'role': 'main',      'lat': 43.6047, 'lon': 1.4442},
    'Lille':           {'country': 'FR', 'role': 'reference', 'lat': 50.6292, 'lon': 3.0573},    # North, wind
    'Brest':           {'country': 'FR', 'role': 'reference', 'lat': 48.3904, 'lon': -4.4861},   # Brittany, Atlantic wind
    'Strasbourg':      {'country': 'FR', 'role': 'reference', 'lat': 48.5734, 'lon': 7.7521},    # Continental northeast
    'Bordeaux':        {'country': 'FR', 'role': 'reference', 'lat': 44.8378, 'lon': -0.5792},   # Southwest Atlantic
    'Montpellier':     {'country': 'FR', 'role': 'reference', 'lat': 43.6108, 'lon': 3.8767},    # Mediterranean, solar

    # ---- Germany ----
    'Berlin':          {'country': 'DE', 'role': 'main',      'lat': 52.5200, 'lon': 13.4050},
    'Hamburg':         {'country': 'DE', 'role': 'main',      'lat': 53.5511, 'lon': 9.9937},
    'Munich':          {'country': 'DE', 'role': 'main',      'lat': 48.1351, 'lon': 11.5820},
    'Cologne':         {'country': 'DE', 'role': 'main',      'lat': 50.9375, 'lon': 6.9603},
    'Emden':           {'country': 'DE', 'role': 'reference', 'lat': 53.3670, 'lon': 7.2060},    # North Sea coast, wind
    'Rostock':         {'country': 'DE', 'role': 'reference', 'lat': 54.0924, 'lon': 12.0991},   # Baltic coast, wind
    'Leipzig':         {'country': 'DE', 'role': 'reference', 'lat': 51.3397, 'lon': 12.3731},   # East, wind + solar
    'Frankfurt':       {'country': 'DE', 'role': 'reference', 'lat': 50.1109, 'lon': 8.6821},    # Centre
    'Stuttgart':       {'country': 'DE', 'role': 'reference', 'lat': 48.7758, 'lon': 9.1829},    # Southwest, solar

    # ---- Italy ----
    'Rome':            {'country': 'IT', 'role': 'main',      'lat': 41.9028, 'lon': 12.4964},
    'Milan':           {'country': 'IT', 'role': 'main',      'lat': 45.4642, 'lon': 9.1900},
    'Naples':          {'country': 'IT', 'role': 'main',      'lat': 40.8518, 'lon': 14.2681},
    'Turin':           {'country': 'IT', 'role': 'main',      'lat': 45.0703, 'lon': 7.6869},
    'Venice':          {'country': 'IT', 'role': 'reference', 'lat': 45.4408, 'lon': 12.3155},   # Northeast
    'Bologna':         {'country': 'IT', 'role': 'reference', 'lat': 44.4949, 'lon': 11.3426},   # Po valley
    'Bari':            {'country': 'IT', 'role': 'reference', 'lat': 41.1171, 'lon': 16.8719},   # Apulia, wind + solar
    'Palermo':         {'country': 'IT', 'role': 'reference', 'lat': 38.1157, 'lon': 13.3615},   # Sicily
    'Cagliari':        {'country': 'IT', 'role': 'reference', 'lat': 39.2238, 'lon': 9.1217},    # Sardinia

    # ---- Netherlands ----
    'Amsterdam':       {'country': 'NL', 'role': 'main',      'lat': 52.3676, 'lon': 4.9041},
    'Rotterdam':       {'country': 'NL', 'role': 'main',      'lat': 51.9244, 'lon': 4.4777},
    'The_Hague':       {'country': 'NL', 'role': 'main',      'lat': 52.0705, 'lon': 4.3007},
    'Den_Helder':      {'country': 'NL', 'role': 'reference', 'lat': 52.9563, 'lon': 4.7601},    # North Sea coast, wind
    'Groningen':       {'country': 'NL', 'role': 'reference', 'lat': 53.2194, 'lon': 6.5665},    # Northeast
    'Eindhoven':       {'country': 'NL', 'role': 'reference', 'lat': 51.4416, 'lon': 5.4697},    # South, inland
    'Maastricht':      {'country': 'NL', 'role': 'reference', 'lat': 50.8514, 'lon': 5.6910},    # Far south

    # ---- Belgium ----
    'Brussels':        {'country': 'BE', 'role': 'main',      'lat': 50.8503, 'lon': 4.3517},
    'Antwerp':         {'country': 'BE', 'role': 'main',      'lat': 51.2194, 'lon': 4.4025},
    'Ghent':           {'country': 'BE', 'role': 'main',      'lat': 51.0543, 'lon': 3.7174},
    'Ostend':          {'country': 'BE', 'role': 'reference', 'lat': 51.2154, 'lon': 2.9286},    # Coast, wind
    'Liege':           {'country': 'BE', 'role': 'reference', 'lat': 50.6326, 'lon': 5.5797},    # East
    'Charleroi':       {'country': 'BE', 'role': 'reference', 'lat': 50.4108, 'lon': 4.4446},    # South, inland
    'Arlon':           {'country': 'BE', 'role': 'reference', 'lat': 49.6833, 'lon': 5.8167},    # Ardennes, far south

    # ---- Austria ----
    'Vienna':          {'country': 'AT', 'role': 'main',      'lat': 48.2082, 'lon': 16.3738},
    'Graz':            {'country': 'AT', 'role': 'main',      'lat': 47.0707, 'lon': 15.4395},
    'Linz':            {'country': 'AT', 'role': 'main',      'lat': 48.3069, 'lon': 14.2858},
    'Eisenstadt':      {'country': 'AT', 'role': 'reference', 'lat': 47.8456, 'lon': 16.5233},   # Burgenland, wind
    'Salzburg':        {'country': 'AT', 'role': 'reference', 'lat': 47.8095, 'lon': 13.0550},   # Alpine foothills
    'Innsbruck':       {'country': 'AT', 'role': 'reference', 'lat': 47.2692, 'lon': 11.4041},   # Alps, west
    'Klagenfurt':      {'country': 'AT', 'role': 'reference', 'lat': 46.6365, 'lon': 14.3122},   # South
    'Bregenz':         {'country': 'AT', 'role': 'reference', 'lat': 47.5031, 'lon': 9.7471},    # Far west
}
