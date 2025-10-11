import os
from flask import Flask, render_template, request, redirect
from owlready2 import *
from rdflib import Graph as RDFGraph, Namespace

# ---------- Locate dataset ----------
CANDIDATES = [
    os.environ.get("GIT_DATASET"),
    "git_dataset.owl", "../git_dataset.owl", "../../git_dataset.owl",
    "data/git_dataset.owl", "../data/git_dataset.owl"
]
DATASET_PATH = next((p for p in CANDIDATES if p and os.path.exists(p)), None)
if not DATASET_PATH:
    raise FileNotFoundError("Could not find git_dataset.owl. Set $GIT_DATASET or place it next to app.py.")

# ---------- Load ontology (Owlready2) ----------
onto = get_ontology(DATASET_PATH).load()
world = onto.world

# ---------- Separate rdflib graph for SPARQL (avoid Owlready2 UNIQUE constraint) ----------
gq = RDFGraph()
gq.parse(DATASET_PATH, format="xml")  # your OWL is RDF/XML

GIT_IRI = "http://example.org/git.owl#"
GIT = Namespace(GIT_IRI)
git = onto.get_namespace(GIT_IRI)

# Detect whether hasParent triples exist in the dataset (drives merge/initial logic)
try:
    _res = list(gq.query("""
    PREFIX git: <http://example.org/git.owl#>
    SELECT (COUNT(*) AS ?parents) WHERE { ?c git:hasParent ?p . }
    """))
    HAS_PARENTS = int(str(_res[0][0])) > 0 if _res else False
except Exception:
    HAS_PARENTS = False

# Try reasoning (optional)
try:
    sync_reasoner()
    REASONER_STATE = "Inferences attempted"
except Exception:
    REASONER_STATE = "Reasoner not available"

app = Flask(__name__)

# ---------- Helpers ----------
def local(x):
    s = str(x)
    if "#" in s: return s.split("#", 1)[1]
    if "/" in s: return s.rsplit("/", 1)[-1]
    return s

def list_instances(cls):
    try: return list(cls.instances())
    except: return []

def inferred_types(ind):
    try:
        return [t for t in ind.INDIRECT_is_a if isinstance(t, ThingClass)]
    except:
        return [t for t in ind.is_a if isinstance(t, ThingClass)]

def find_repo_by_name(name: str):
    """Find a Repository individual by repoFullName string."""
    for r in list_instances(onto.Repository):
        vals = getattr(r, "repoFullName", [])
        if vals and vals[0] == name:
            return r
    return None

def validate_graph():
    errs = []
    for r in list_instances(onto.Repository):
        if len(getattr(r, "hasBranch", [])) < 1:
            errs.append({"where": getattr(r, "repoFullName", [local(r)])[0],
                         "rule": "Repository must have ≥1 branch",
                         "fix": "Add Branch via hasBranch"})
    for b in list_instances(onto.Branch):
        inits = getattr(b, "hasInitialCommit", None)
        commits = getattr(b, "hasCommit", [])
        name = getattr(b, "branchName", ["<unnamed>"])[0]
        if not inits:
            errs.append({"where": f"Branch {name}",
                         "rule": "Branch must have an initial commit",
                         "fix": "Link via hasInitialCommit"})
        if len(commits) < 1:
            errs.append({"where": f"Branch {name}",
                         "rule": "Branch must have ≥1 commit",
                         "fix": "Attach commits via hasCommit"})
    return errs

# ---------- Search parsing ----------
def parse_query(qs: str):
    parts = qs.strip().split()
    tokens = {}
    i = 0
    while i < len(parts):
        p = parts[i]
        if ":" in p:
            k, v = p.split(":", 1)
            k = k.lower().strip()
            v = v.strip()
            # If value is empty and there's a next token without ":", use it
            if v == "" and i + 1 < len(parts) and ":" not in parts[i + 1]:
                v = parts[i + 1].strip()
                i += 1  # consume the next token as the value
            if v:  # only store non-empty values
                tokens[k] = v
        else:
            # free text defaults to message filter
            tokens.setdefault("msg", p)
        i += 1
    return tokens

def build_sparql(tokens):
    prefix = 'PREFIX git: <http://example.org/git.owl#>\n'
    ent = tokens.get("type", "commit").lower()
    limit = int(tokens.get("limit", "50")) if tokens.get("limit") else 50

    # Default commit view (columns)
    select = "SELECT DISTINCT ?repoName ?branchName ?commit ?msg ?authorName\n"

    # Core graph pattern; NOTE trailing dot on the last OPTIONAL keeps rdflib happy
    core = [
        "?branch a git:Branch ; git:hasCommit ?commit .",
        "OPTIONAL { ?commit git:commitMessage ?msg . }",
        "OPTIONAL { ?branch git:branchName ?branchName . }",
        "?repo a git:Repository ; git:hasBranch ?branch .",
        "OPTIONAL { ?repo git:repoFullName ?repoName . }",
        "OPTIONAL { ?commit git:madeBy ?user . OPTIONAL { ?user git:userLogin ?authorName . }} .",
    ]

    where = []

    # ----- Entity-type constraints -----
    if ent == "merge":
        if HAS_PARENTS:
            where.append("""
            {
              ?commit a git:MergeCommit .
            } UNION {
              ?commit git:hasParent ?p1 ;
                      git:hasParent ?p2 .
              FILTER(?p1 != ?p2)
            } UNION {
              FILTER(BOUND(?msg) && REGEX(LCASE(STR(?msg)), "^(merge( pull request)?|merge branch|merged )"))
            }
            """.strip())
        else:
            # No parent edges: rely on message heuristic only
            where.append("""
            FILTER(BOUND(?msg) && REGEX(LCASE(STR(?msg)), "^(merge( pull request)?|merge branch|merged )"))
            """.strip())

    if ent == "initial":
        if HAS_PARENTS:
            where.append("""
            {
              ?commit a git:InitialCommit .
            } UNION {
              FILTER NOT EXISTS { ?commit git:hasParent ?anyParent . }
            }
            """.strip())
        else:
            # Without parents, only explicit typing is possible
            where.append("?commit a git:InitialCommit .")

    # ----- Filters (BOUND-safe; ignore empty tokens) -----
    if tokens.get("msg"):
        where.append(
            f'FILTER(BOUND(?msg) && CONTAINS(LCASE(STR(?msg)), "{tokens["msg"].lower()}"))'
        )

    # Author filter with fallback to IRI localname if userLogin missing
    if tokens.get("author"):
        author = tokens["author"].lower().replace('"', '\\"')
        # Ensure ?user is bound (mandatory when author filter is used)
        where.append("?commit git:madeBy ?user .")
        # Try to get explicit login if present
        where.append("OPTIONAL { ?user git:userLogin ?authorName . }")
        # Compute a comparable key: explicit login if present, else localname of the IRI
        where.append(
            "BIND(LCASE(IF(BOUND(?authorName), STR(?authorName), "
            "REPLACE(STR(?user), '^(.*[#/])', ''))) AS ?authorKey)"
        )
        where.append(f'FILTER(?authorKey = "{author}")')

    if tokens.get("branch"):
        where.append(
            f'FILTER(BOUND(?branchName) && STR(?branchName) = "{tokens["branch"]}")'
        )

    if tokens.get("repo"):
        where.append(
            f'FILTER(BOUND(?repoName) && CONTAINS(LCASE(STR(?repoName)), "{tokens["repo"].lower()}"))'
        )

    q = prefix + select + "WHERE {\n  " + "\n  ".join(core + where) + "\n}\n" + f"LIMIT {limit}\n"
    return q, ent

# ---------- Routes ----------
@app.route("/")
def home():
    counts = {
        "repos": len(list_instances(onto.Repository)),
        "branches": len(list_instances(onto.Branch)),
        "commits": len(list_instances(onto.Commit)),
        "users": len(list_instances(onto.User)),
        "files": len(list_instances(onto.File)) if hasattr(onto, "File") else 0,
        "reasoner": REASONER_STATE,
        "dataset": DATASET_PATH
    }
    return render_template("home.html", counts=counts)

@app.route("/browse")
def browse():
    repos = list_instances(onto.Repository)
    rows = []
    for r in repos:
        # Prefer repoFullName (string). Fallback to local IRI.
        if hasattr(r, "repoFullName") and getattr(r, "repoFullName", None):
            label_text = r.repoFullName[0] if isinstance(r.repoFullName, list) else r.repoFullName
        else:
            label_text = local(r)

        branches = getattr(r, "hasBranch", [])
        commits = []
        for b in branches:
            commits.extend(getattr(b, "hasCommit", []))

        rows.append({
            "iri": local(r),
            "name": str(label_text),
            "branches": len(branches),
            "commits": len(set(commits)),
        })

    rows.sort(key=lambda t: t["name"].lower())
    return render_template("browse.html", rows=rows)


@app.route("/entity")
def entity():
    """Resolve entity by ?name=<repoFullName> first; fallback to ?iri=<local IRI>."""
    iri  = request.args.get("iri", "")
    name = request.args.get("name", "")

    ind = None
    if name:
        ind = find_repo_by_name(name)

    if ind is None and iri:
        ind = onto.search_one(iri=GIT_IRI + iri)

    if ind is None:
        # last-resort: try matching by rdfs:label (some individuals may have it)
        for i in onto.individuals():
            if getattr(i, "label", []) and i.label[0] == name:
                ind = i
                break

    if ind is None:
        return render_template("entity.html", notfound=True)

    asserted = [t for t in ind.is_a if isinstance(t, ThingClass)]
    inferred = [t for t in inferred_types(ind) if t not in asserted]

    obj_rows, data_rows = [], []
    for p in onto.object_properties():
        vals = getattr(ind, p.python_name, [])
        if vals and not isinstance(vals, list): vals = [vals]
        for v in (vals or []):
            obj_rows.append((p.name, getattr(v, "label", [local(v)])[0]))

    for p in onto.data_properties():
        vals = getattr(ind, p.python_name, [])
        if vals and not isinstance(vals, list): vals = [vals]
        for v in (vals or []):
            data_rows.append((p.name, v))

    # Nice display name: repoFullName > rdfs:label > local IRI
    display_name = getattr(ind, "repoFullName", None)
    display_name = display_name[0] if display_name else getattr(ind, "label", [local(ind)])[0]

    return render_template(
        "entity.html",
        name=display_name,
        iri=local(ind),
        asserted=asserted,
        inferred=inferred,
        obj_rows=obj_rows,
        data_rows=data_rows
    )

@app.route("/search")
def search():
    qs = request.args.get("q", "").strip()
    qtxt, rows = None, []

    if not qs:
        return render_template("search.html", qs=qs, qtxt=qtxt, rows=rows)

    # Detect raw SPARQL (user pasted a full query)
    is_raw_sparql = qs.lower().startswith("prefix") or ("select" in qs.lower()) or ("where" in qs.lower())

    # Small inline parser that correctly handles "branch: main" (value in next token)
    def _smart_tokens(text: str):
        parts = text.strip().split()
        tokens = {}
        i = 0
        while i < len(parts):
            p = parts[i]
            if ":" in p:
                k, v = p.split(":", 1)
                k = k.lower().strip()
                v = v.strip()
                # If value missing and next token has no ":", treat that as the value
                if v == "" and i + 1 < len(parts) and ":" not in parts[i + 1]:
                    v = parts[i + 1].strip()
                    i += 1  # consume the next token
                if v:  # only record non-empty values
                    tokens[k] = v
            else:
                # free text defaults to msg:
                tokens.setdefault("msg", p)
            i += 1
        return tokens

    try:
        if is_raw_sparql:
            qtxt = qs
        else:
            tokens = _smart_tokens(qs)
            qtxt, _ = build_sparql(tokens)

        # Run query on the pure rdflib graph
        result = gq.query(qtxt, initNs={"git": GIT})
        for r in result:
            if r is None:
                continue
            try:
                rows.append([str(c) if c is not None else "" for c in r])
            except TypeError:
                rows.append(["Error parsing row", str(r)])
    except Exception as e:
        rows = [["Query error", str(e)]]

    return render_template("search.html", qs=qs, qtxt=qtxt, rows=rows)

@app.route("/errors")
def errors():
    errs = validate_graph()
    return render_template("errors.html", errs=errs)

if __name__ == "__main__":
    app.run(debug=True)
