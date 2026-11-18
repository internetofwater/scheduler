import random
import sys

random.seed(7)
N = int(sys.argv[1])
H = "https://www.opengis.net/def/schema/hy_features/hyf/"
S = "https://schema.org/"
G = "http://www.opengis.net/ont/geosparql#"
W = f"^^<{G}wktLiteral>"
types = [
    "Stream",
    "Well",
    "Lake",
    "Spring",
    "Reservoir",
    "Canal",
    "Estuary",
    "Atmosphere",
]
tw = [50, 30, 6, 5, 3, 3, 2, 1]
provs = [f"Provider {i}" for i in range(15)]
vars_ = [
    "Temperature, water",
    "pH",
    "Discharge",
    "Gage height",
    "Specific conductance",
    "Dissolved oxygen",
    "Turbidity",
] + [f"Variable {i}" for i in range(293)]
vw = [1 / (i + 1) for i in range(len(vars_))]
nm = 20000
mw = [1 / ((i + 1) ** 0.9) for i in range(nm)]
mains = random.choices(range(1, nm + 1), weights=mw, k=N)
out = sys.stdout
w = out.write
bn = 0
for i in range(N):
    L = f"<https://geoconnex.us/ref/sites/{i}>"
    m = mains[i]
    b1 = f"_:a{i}"
    b2 = f"_:b{i}"
    g = f"_:g{i}"
    p = f"_:p{i}"
    t = random.choices(types, tw)[0]
    w(f'{L} <{H}HydroLocationType> "{t}" .\n')
    w(
        f"{L} <{H}referencedPosition> {b1} .\n{b1} <{H}HY_IndirectPosition> {b2} .\n{b2} <{H}linearElement> <https://geoconnex.us/ref/mainstems/{m}> .\n"
    )
    w(
        f'{L} <{G}hasGeometry> {g} .\n{g} <{G}asWKT> "POINT({random.uniform(-125, -67):.6f} {random.uniform(25, 49):.6f})"{W} .\n'
    )
    w(f'{L} <{S}provider> {p} .\n{p} <{S}name> "{random.choice(provs)}" .\n')
    for d in range(random.choice([1, 1, 1, 2, 3])):
        D = f"<https://example.org/ds/{i}_{d}>"
        w(f"{L} <{S}subjectOf> {D} .\n")
        w(
            f'{D} <{S}name> "Site {i}" .\n{D} <{S}description> "Dataset {i}/{d} description" .\n{D} <{S}temporalCoverage> "{random.randint(1950, 2010)}-01-01/{random.randint(2011, 2025)}-01-01" .\n'
        )
        for v in random.choices(vars_, vw, k=random.choice([1, 2, 3, 4])):
            bn += 1
            V = f"_:v{bn}"
            w(
                f'{D} <{S}variableMeasured> {V} .\n{V} <{S}name> "{v}" .\n{V} <{S}unitText> "unit" .\n'
            )
# noise: other geoconnex features that are not monitoring locations
for i in range(N // 2):
    w(
        f'<https://geoconnex.us/ref/other/{i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <{S}Place> .\n<https://geoconnex.us/ref/other/{i}> <{S}name> "Other {i}" .\n<https://geoconnex.us/ref/other/{i}> <{S}description> "Other feature {i}" .\n'
    )
