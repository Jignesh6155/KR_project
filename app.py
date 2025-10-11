from flask import Flask, render_template, request
from owlready2 import *
from datetime import datetime

app = Flask(__name__)

# Path to your ontology dataset (adjust if needed)
ONTO_PATH = "../git_dataset.owl"

# ---------- LOAD ONTOLOGY ----------
onto = get_ontology(ONTO_PATH).load()
Repository = onto.Repository
Branch = onto.Branch
Commit = onto.Commit
User = onto.User
InitialCommit = getattr(onto, "InitialCommit", None)
MergeCommit = getattr(onto, "MergeCommit", None)

# Try reasoning (optional)
def try_reason():
    try:
        sync_reasoner()  # Java-based reasoner; OK if it fails
        return True
    except Exception:
        return False

REASONER_OK = try_reason()

# ---------- HELPER FUNCTIONS ----------
def is_initial(c):
    if InitialCommit and isinstance(c, InitialCommit):
        return True
    return len(getattr(c, "hasParent", [])) == 0

def is_merge(c):
    if MergeCommit and isinstance(c, MergeCommit):
        return True
    return len(getattr(c, "hasParent", [])) >= 2

def commit_author(c):
    a = getattr(c, "madeBy", None)
    if not a:
        return "(unknown)"
    if isinstance(a, list) and a:
        return a[0].name if hasattr(a[0], "name") else str(a[0])
    if hasattr(a, "name"):
        return a.name
    return str(a)

def commit_msg(c):
    m = getattr(c, "commitMessage", None)
    if isinstance(m, list) and m:
        return m[0]
    return m or ""

def commit_ts(c):
    ts = getattr(c, "commitTimestamp", None)
    if not ts:
        return ""
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, list) and ts:
        t0 = ts[0]
        return t0.isoformat() if isinstance(t0, datetime) else str(t0)
    return str(ts)

def branch_label(b):
    name = getattr(b, "branchName", [])
    if isinstance(name, list) and name:
        return name[0]
    return name or "(unnamed)"

def repo_by_name(rid):
    return getattr(onto, rid, None)

def branch_by_label(name):
    for b in Branch.instances():
        if getattr(b, "branchName", None) and b.branchName and b.branchName[0] == name:
            return b
    return None

# ---------- VALIDATION ----------
def validate_graph():
    issues = []

    # Repositories must have ≥1 branch
    for r in Repository.instances():
        if not getattr(r, "hasBranch", []):
            issues.append(("Repository", r.name, "Repository has no branches"))

    # Branch rules
    for b in Branch.instances():
        names = getattr(b, "branchName", [])
        inits = getattr(b, "hasInitialCommit", [])
        commits = getattr(b, "hasCommit", [])
        if len(names) != 1:
            issues.append(("Branch", branch_label(b), f"branchName count != 1 (got {len(names)})"))
        if len(inits) != 1:
            issues.append(("Branch", branch_label(b), f"initialCommit count != 1 (got {len(inits)})"))
        if len(commits) < 1:
            issues.append(("Branch", branch_label(b), "Branch has no commits"))

    # Commit rules
    for c in Commit.instances():
        errs = []
        if not getattr(c, "madeBy", None) or not c.madeBy:
            errs.append("Missing author")
        if not getattr(c, "commitTimestamp", None) or not c.commitTimestamp:
            errs.append("Missing timestamp")
        if not getattr(c, "commitMessage", None) or not c.commitMessage:
            errs.append("Missing message")
        if not getattr(c, "updatesFile", None) or not c.updatesFile:
            errs.append("Missing updated files")

        parents = getattr(c, "hasParent", [])
        if is_initial(c) and parents:
            errs.append("Initial commit has parent(s)")
        if is_merge(c) and len(parents) < 2:
            errs.append("Merge commit has <2 parents")
        if (not is_initial(c)) and len(parents) < 1:
            errs.append("Non-initial commit missing parent")

        if errs:
            issues.append(("Commit", c.name, "; ".join(errs)))

    return issues

# ---------- SEARCH DSL ----------
def parse_query(q):
    parsed = {"msg": None, "author": None, "type": None, "branch": None, "repo": None, "limit": 100}
    free = []
    for t in q.strip().split():
        if ":" in t:
            k, v = t.split(":", 1)
            k = k.lower().strip()
            v = v.strip().strip('"').strip("'")
            if k == "limit":
                try:
                    parsed["limit"] = int(v)
                except ValueError:
                    pass
            elif k in parsed:
                parsed[k] = v
        else:
            free.append(t)
    if free and not parsed["msg"]:
        parsed["msg"] = " ".join(free)
    return parsed

def search_commits(parsed):
    results = []
    r_filter = repo_by_name(parsed["repo"]) if parsed["repo"] else None
    b_filter = branch_by_label(parsed["branch"]) if parsed["branch"] else None

    for c in Commit.instances():
        # Filter by repository membership
        if r_filter:
            in_repo = False
            for br in getattr(r_filter, "hasBranch", []):
                if getattr(br, "hasCommit", []) and c in br.hasCommit:
                    in_repo = True
                    break
            if not in_repo:
                continue

        # Branch filter
        if b_filter:
            if not getattr(b_filter, "hasCommit", []) or c not in b_filter.hasCommit:
                continue

        # Type filter
        if parsed["type"]:
            typ = parsed["type"].lower()
            if typ == "merge" and not is_merge(c):
                continue
            if typ in ("initial", "root") and not is_initial(c):
                continue
            if typ in ("normal", "regular") and (is_merge(c) or is_initial(c)):
                continue

        # Author filter
        if parsed["author"] and parsed["author"].lower() not in commit_author(c).lower():
            continue

        # Message filter
        if parsed["msg"] and parsed["msg"].lower() not in commit_msg(c).lower():
            continue

        results.append(c)
        if len(results) >= parsed["limit"]:
            break

    return results

# ---------- ROUTES ----------
@app.route("/")
def index():
    repos = []
    for r in Repository.instances():
        brs = getattr(r, "hasBranch", [])
        commit_count = sum(len(getattr(b, "hasCommit", [])) for b in brs)
        repos.append({
            "iri": r.name,
            "branches": len(brs),
            "commits": commit_count
        })
    return render_template("index.html", repos=repos, app_title="Git-Onto-Logic", reasoner=REASONER_OK)

@app.route("/repo/<rid>")
def repo_view(rid):
    r = repo_by_name(rid)
    if not r:
        return render_template("message.html",
                               app_title="Git-Onto-Logic",
                               title="Not found",
                               message=f"Repository '{rid}' not found."), 404
    branches = []
    for b in getattr(r, "hasBranch", []):
        cdata = []
        for c in getattr(b, "hasCommit", []):
            cdata.append({
                "name": c.name,
                "author": commit_author(c),
                "ts": commit_ts(c),
                "msg": commit_msg(c),
                "is_merge": is_merge(c),
                "is_initial": is_initial(c),
                "parents": [p.name for p in getattr(c, "hasParent", [])],
            })
        branches.append({
            "name": branch_label(b),
            "raw": b.name,
            "initial": (getattr(b, "hasInitialCommit", [None])[0].name if getattr(b, "hasInitialCommit", []) else None),
            "commit_count": len(cdata),
            "commits": cdata
        })
    return render_template("repo.html", repo=r, branches=branches, app_title="Git-Onto-Logic", reasoner=REASONER_OK)

@app.route("/search")
def search():
    q = request.args.get("q", "").strip()
    parsed = parse_query(q) if q else {}
    results = search_commits(parsed) if q else []
    return render_template("search.html",
                           q=q,
                           parsed=parsed,
                           results=results,
                           app_title="Git-Onto-Logic",
                           reasoner=REASONER_OK)

@app.route("/validate")
def validate():
    issues = validate_graph()
    return render_template("validate.html",
                           issues=issues,
                           app_title="Git-Onto-Logic",
                           reasoner=REASONER_OK)

@app.route("/about")
def about():
    return render_template("message.html",
                           app_title="Git-Onto-Logic",
                           title="About",
                           message="Browse and search the Git ontology knowledge graph. Displays inferred classes and validates against ontology constraints.")

# ---------- RUN ----------
if __name__ == "__main__":
    app.run(debug=True)
