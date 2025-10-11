import os
from flask import Flask, render_template, request, redirect
from owlready2 import *

# ---------- Locate dataset ----------
CANDIDATES = [
    os.environ.get("GIT_DATASET"),
    "git_dataset.owl", "../git_dataset.owl", "../../git_dataset.owl",
    "data/git_dataset.owl", "../data/git_dataset.owl"
]
DATASET_PATH = next((p for p in CANDIDATES if p and os.path.exists(p)), None)
if not DATASET_PATH:
    raise FileNotFoundError("Could not find git_dataset.owl. Set $GIT_DATASET or place it next to app.py.")

# ---------- Load ontology / graph ----------
onto = get_ontology(DATASET_PATH).load()
world = onto.world
g = world.as_rdflib_graph()
GIT_IRI = "http://example.org/git.owl#"
git = onto.get_namespace(GIT_IRI)

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
    for p in parts:
        if ":" in p:
            k, v = p.split(":", 1)
            tokens[k.lower()] = v
        else:
            tokens.setdefault("msg", p)
    return tokens

def build_sparql(tokens):
    prefix = 'PREFIX git: <http://example.org/git.owl#>\n'
    ent = tokens.get("type", "commit").lower()
    limit = int(tokens.get("limit", "50")) if tokens.get("limit") else 50
    select = "SELECT DISTINCT ?commit ?msg ?branchName ?repoName ?authorName\n"
    core = [
        "?branch a git:Branch ; git:hasCommit ?commit .",
        "OPTIONAL { ?commit git:commitMessage ?msg . }",
        "OPTIONAL { ?branch git:branchName ?branchName . }",
        "?repo a git:Repository ; git:hasBranch ?branch .",
        "OPTIONAL { ?repo git:repoFullName ?repoName . }",
        "OPTIONAL { ?commit git:madeBy ?user . OPTIONAL { ?user git:userLogin ?authorName . }}",
    ]
    where = []
    if ent == "merge": where.append("?commit a git:MergeCommit .")
    if ent == "initial": where.append("?commit a git:InitialCommit .")
    if "msg" in tokens: where.append(f'FILTER (CONTAINS(LCASE(STR(?msg)), "{tokens["msg"].lower()}"))')
    if "author" in tokens: where.append(f'FILTER (LCASE(STR(?authorName)) = "{tokens["author"].lower()}")')
    if "branch" in tokens: where.append(f'FILTER (STR(?branchName) = "{tokens["branch"]}")')
    if "repo" in tokens: where.append(f'FILTER (CONTAINS(LCASE(STR(?repoName)), "{tokens["repo"].lower()}"))')
    q = prefix + select + "WHERE {\n  " + "\n  ".join(core + where) + f"\n}}\nLIMIT {limit}\n"
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
        branches = getattr(r, "hasBranch", [])
        commits = []
        for b in branches:
            commits.extend(getattr(b, "hasCommit", []))
        label = getattr(r, "repoFullName", [local(r)])[0] if hasattr(r, "repoFullName") else local(r)
        rows.append((local(r), label, len(branches), len(set(commits))))
    rows.sort(key=lambda t: t[1].lower())
    return render_template("browse.html", rows=rows)

@app.route("/entity")
def entity():
    iri = request.args.get("iri", "")
    if not iri: return redirect("/browse")
    ind = onto.search_one(iri=GIT_IRI + iri)
    if not ind: return render_template("entity.html", notfound=True)
    asserted = [t for t in ind.is_a if isinstance(t, ThingClass)]
    inferred = [t for t in inferred_types(ind) if t not in asserted]
    obj_rows, data_rows = [], []
    for p in onto.object_properties():
        vals = getattr(ind, p.python_name, [])
        if vals and not isinstance(vals, list): vals = [vals]
        for v in (vals or []): obj_rows.append((p.name, getattr(v, "label", [local(v)])[0]))
    for p in onto.data_properties():
        vals = getattr(ind, p.python_name, [])
        if vals and not isinstance(vals, list): vals = [vals]
        for v in (vals or []): data_rows.append((p.name, v))
    return render_template("entity.html", name=getattr(ind, "label", [local(ind)])[0],
                           iri=local(ind), asserted=asserted, inferred=inferred,
                           obj_rows=obj_rows, data_rows=data_rows)

@app.route("/search")
def search():
    qs = request.args.get("q", "").strip()
    qtxt, rows = None, []
    ent = "commit"
    if qs:
        tokens = parse_query(qs)
        qtxt, ent = build_sparql(tokens)
        try:
            result = g.query(qtxt, initNs={"git": GIT_IRI})
            for r in result:
                rows.append([str(c) if c is not None else "" for c in r])
        except Exception as e:
            rows = [["Query error", str(e)]]
    return render_template("search.html", qs=qs, qtxt=qtxt, rows=rows, ent=ent)

@app.route("/errors")
def errors():
    errs = validate_graph()
    return render_template("errors.html", errs=errs)

if __name__ == "__main__":
    app.run(debug=True)
