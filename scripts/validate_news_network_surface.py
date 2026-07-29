from __future__ import annotations

import argparse
import ast
from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any


DEFAULT_TARGETS = (
    "app/providers/news_provider.py",
    "app/services/news_intelligence_service.py",
)

LOCAL_ONLY_MODULES = {
    "urllib.parse": (
        "Python standard-library URL parsing and serialization only; "
        "the module exposes no socket or HTTP transport."
    ),
}

UNOBSERVED_NETWORK_MODULES = {
    "aiohttp",
    "boto3",
    "botocore",
    "ftplib",
    "grpc",
    "http.client",
    "httpcore",
    "requests",
    "smtplib",
    "socket",
    "urllib.request",
    "urllib3",
    "websocket",
    "websockets",
}

HTTPX_NETWORK_METHODS = {
    "delete",
    "get",
    "head",
    "options",
    "patch",
    "post",
    "put",
    "request",
    "send",
    "stream",
}

HTTPX_LOCAL_CALLABLES = {
    "httpx.AsyncClient",
    "httpx.Client",
    "httpx.Headers",
    "httpx.Limits",
    "httpx.QueryParams",
    "httpx.Request",
    "httpx.Response",
    "httpx.Timeout",
    "httpx.URL",
}

HTTPX_OBSERVERS = {
    "httpx.AsyncClient": (
        "httpx.AsyncClient.send",
        "httpx.AsyncHTTPTransport.handle_async_request",
    ),
    "httpx.Client": (
        "httpx.Client.send",
        "httpx.HTTPTransport.handle_request",
    ),
    "httpx": (
        "httpx.Client.send",
        "httpx.HTTPTransport.handle_request",
    ),
}


@dataclass(frozen=True)
class SurfaceEntry:
    library: str
    module: str
    callable: str
    file: str
    function: str
    line: int
    classification: str
    reason_code: str
    reason: str
    observed_by: tuple[str, ...] = ()


def _attribute_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _attribute_name(node.value)
        if parent:
            return f"{parent}.{node.attr}"
    return None


def _annotation_name(node: ast.AST | None, aliases: dict[str, str]) -> str | None:
    raw = _attribute_name(node) if node is not None else None
    if not raw:
        return None
    root, separator, suffix = raw.partition(".")
    resolved = aliases.get(root, root)
    return f"{resolved}.{suffix}" if separator else resolved


def _resolve_name(
    raw: str,
    *,
    aliases: dict[str, str],
    variable_types: dict[str, str],
) -> str:
    root, separator, suffix = raw.partition(".")
    resolved_root = variable_types.get(root) or aliases.get(root) or root
    return f"{resolved_root}.{suffix}" if separator else resolved_root


def _module_for_callable(callable_name: str) -> str:
    if callable_name.startswith("urllib.parse."):
        return "urllib.parse"
    if callable_name.startswith("urllib.request."):
        return "urllib.request"
    if callable_name.startswith("http.client."):
        return "http.client"
    return callable_name.split(".", 1)[0]


def _library_for_module(module: str) -> str:
    return module.split(".", 1)[0]


def _matches_module(module: str, candidates: set[str]) -> bool:
    return any(module == item or module.startswith(f"{item}.") for item in candidates)


class NetworkSurfaceVisitor(ast.NodeVisitor):
    def __init__(self, *, relative_path: str) -> None:
        self.relative_path = relative_path
        self.aliases: dict[str, str] = {}
        self.scope_types: list[dict[str, str]] = [{}]
        self.function_names: list[str] = ["<module>"]
        self.network_paths: list[SurfaceEntry] = []
        self.local_only_paths: list[SurfaceEntry] = []
        self.violations: list[SurfaceEntry] = []

    @property
    def variable_types(self) -> dict[str, str]:
        merged: dict[str, str] = {}
        for scope in self.scope_types:
            merged.update(scope)
        return merged

    @property
    def function_name(self) -> str:
        qualified = [
            name
            for name in self.function_names
            if name != "<module>"
        ]
        return ".".join(qualified) if qualified else "<module>"

    def visit_Import(self, node: ast.Import) -> Any:
        for alias in node.names:
            self.aliases[alias.asname or alias.name.split(".", 1)[0]] = alias.name
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> Any:
        if not node.module:
            self.generic_visit(node)
            return
        for alias in node.names:
            if alias.name == "*":
                self.violations.append(
                    SurfaceEntry(
                        library=_library_for_module(node.module),
                        module=node.module,
                        callable="*",
                        file=self.relative_path,
                        function=self.function_name,
                        line=node.lineno,
                        classification="UNOBSERVED_NETWORK_SURFACE",
                        reason_code="NETWORK_WILDCARD_IMPORT_NOT_AUDITABLE",
                        reason=(
                            f"Wildcard import from {node.module} cannot be tied "
                            "to a concrete intercepted callable."
                        ),
                    )
                )
                continue
            self.aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
        self.generic_visit(node)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        scope: dict[str, str] = {}
        arguments = [
            *node.args.posonlyargs,
            *node.args.args,
            *node.args.kwonlyargs,
        ]
        for argument in arguments:
            annotation = _annotation_name(argument.annotation, self.aliases)
            if annotation:
                scope[argument.arg] = annotation
        self.scope_types.append(scope)
        self.function_names.append(node.name)
        for statement in node.body:
            self.visit(statement)
        self.function_names.pop()
        self.scope_types.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
        self._visit_function(node)

    def _record_assignment(self, target: ast.AST, value: ast.AST) -> None:
        if not isinstance(target, ast.Name):
            return
        inferred: str | None = None
        if isinstance(value, ast.Call):
            raw = _attribute_name(value.func)
            if raw:
                inferred = _resolve_name(
                    raw,
                    aliases=self.aliases,
                    variable_types=self.variable_types,
                )
        elif isinstance(value, ast.Name):
            inferred = self.variable_types.get(value.id)
        inferred_module = _module_for_callable(inferred) if inferred else ""
        inferred_factory = inferred.rsplit(".", 1)[-1] if inferred else ""
        is_network_client = (
            inferred in {"httpx.AsyncClient", "httpx.Client"}
            or (
                bool(inferred)
                and _matches_module(inferred_module, UNOBSERVED_NETWORK_MODULES)
                and inferred_factory
                in {"Client", "ClientSession", "PoolManager", "Session"}
            )
        )
        if inferred and is_network_client:
            self.scope_types[-1][target.id] = inferred

    def visit_Assign(self, node: ast.Assign) -> Any:
        for target in node.targets:
            self._record_assignment(target, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> Any:
        if isinstance(node.target, ast.Name):
            annotation = _annotation_name(node.annotation, self.aliases)
            if annotation:
                self.scope_types[-1][node.target.id] = annotation
            elif node.value is not None:
                self._record_assignment(node.target, node.value)
        self.generic_visit(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> Any:
        for item in node.items:
            if item.optional_vars is not None:
                self._record_assignment(item.optional_vars, item.context_expr)
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> Any:
        for item in node.items:
            if item.optional_vars is not None:
                self._record_assignment(item.optional_vars, item.context_expr)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> Any:
        raw = _attribute_name(node.func)
        if not raw:
            self.generic_visit(node)
            return
        callable_name = _resolve_name(
            raw,
            aliases=self.aliases,
            variable_types=self.variable_types,
        )
        module = _module_for_callable(callable_name)
        library = _library_for_module(module)

        if _matches_module(module, set(LOCAL_ONLY_MODULES)):
            matched_module = next(
                item
                for item in LOCAL_ONLY_MODULES
                if module == item or module.startswith(f"{item}.")
            )
            self.local_only_paths.append(
                SurfaceEntry(
                    library=library,
                    module=matched_module,
                    callable=callable_name,
                    file=self.relative_path,
                    function=self.function_name,
                    line=node.lineno,
                    classification="LOCAL_ONLY",
                    reason_code="NO_NETWORK_CAPABILITY",
                    reason=LOCAL_ONLY_MODULES[matched_module],
                )
            )
        elif callable_name.startswith("httpx."):
            self._classify_httpx(node=node, callable_name=callable_name)
        elif _matches_module(module, UNOBSERVED_NETWORK_MODULES):
            self.violations.append(
                SurfaceEntry(
                    library=library,
                    module=module,
                    callable=callable_name,
                    file=self.relative_path,
                    function=self.function_name,
                    line=node.lineno,
                    classification="UNOBSERVED_NETWORK_SURFACE",
                    reason_code="NETWORK_CALLABLE_NOT_INTERCEPTED",
                    reason=(
                        f"{callable_name} can perform network I/O but is not "
                        "covered by the validation harness allowlist and audit."
                    ),
                )
            )
        self.generic_visit(node)

    def _classify_httpx(self, *, node: ast.Call, callable_name: str) -> None:
        parts = callable_name.split(".")
        method = parts[-1]
        owner = ".".join(parts[:-1])
        if callable_name in HTTPX_LOCAL_CALLABLES:
            self.local_only_paths.append(
                SurfaceEntry(
                    library="httpx",
                    module="httpx",
                    callable=callable_name,
                    file=self.relative_path,
                    function=self.function_name,
                    line=node.lineno,
                    classification="CLIENT_OR_VALUE_CONSTRUCTION",
                    reason_code="NO_NETWORK_UNTIL_METHOD_CALL",
                    reason=(
                        f"{callable_name} constructs a client or local HTTP "
                        "value; outbound I/O is audited at send and transport."
                    ),
                )
            )
            return
        if method in HTTPX_NETWORK_METHODS:
            observer_owner = owner
            if owner not in HTTPX_OBSERVERS:
                observer_owner = "httpx"
            observers = HTTPX_OBSERVERS.get(observer_owner, ())
            self.network_paths.append(
                SurfaceEntry(
                    library="httpx",
                    module="httpx",
                    callable=callable_name,
                    file=self.relative_path,
                    function=self.function_name,
                    line=node.lineno,
                    classification="OBSERVED_NETWORK_SURFACE",
                    reason_code="INTERCEPTED_FAIL_CLOSED",
                    reason=(
                        f"{callable_name} routes through the audited httpx send "
                        "and transport layers."
                    ),
                    observed_by=observers,
                )
            )
            return
        self.violations.append(
            SurfaceEntry(
                library="httpx",
                module="httpx",
                callable=callable_name,
                file=self.relative_path,
                function=self.function_name,
                line=node.lineno,
                classification="UNOBSERVED_NETWORK_SURFACE",
                reason_code="HTTPX_CALLABLE_CLASSIFICATION_MISSING",
                reason=(
                    f"{callable_name} belongs to a network-capable library but "
                    "is neither proven local-only nor mapped to an interceptor."
                ),
            )
        )


def analyze_source(*, source: str, relative_path: str) -> dict[str, Any]:
    tree = ast.parse(source, filename=relative_path)
    visitor = NetworkSurfaceVisitor(relative_path=relative_path)
    visitor.visit(tree)
    return {
        "network_paths": [asdict(item) for item in visitor.network_paths],
        "local_only_paths": [asdict(item) for item in visitor.local_only_paths],
        "violations": [asdict(item) for item in visitor.violations],
    }


def analyze_news_network_surface(
    repo_root: Path,
    *,
    targets: tuple[str, ...] = DEFAULT_TARGETS,
) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    network_paths: list[dict[str, Any]] = []
    local_only_paths: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    for relative_path in targets:
        target = repo_root / relative_path
        result = analyze_source(
            source=target.read_text(encoding="utf-8"),
            relative_path=relative_path,
        )
        files.append(
            {
                "path": relative_path,
                "sha256": _sha256(target),
            }
        )
        network_paths.extend(result["network_paths"])
        local_only_paths.extend(result["local_only_paths"])
        violations.extend(result["violations"])

    required_provider_functions = {
        "_fetch_alpha_vantage",
        "_fetch_gdelt",
        "_fetch_one_rss_feed",
        "_enrich_missing_metadata",
    }
    network_callsite_functions = {
        str(item["function"])
        for item in network_paths
        if item["file"] == "app/providers/news_provider.py"
    }
    observed_provider_functions = {
        required
        for required in required_provider_functions
        if any(
            function == required or function.startswith(f"{required}.")
            for function in network_callsite_functions
        )
    }
    missing_provider_functions = sorted(
        required_provider_functions - observed_provider_functions
    )
    for function in missing_provider_functions:
        violations.append(
            asdict(
                SurfaceEntry(
                    library="httpx",
                    module="app.providers.news_provider",
                    callable=f"{function}:httpx.AsyncClient.get",
                    file="app/providers/news_provider.py",
                    function=function,
                    line=0,
                    classification="UNOBSERVED_NETWORK_SURFACE",
                    reason_code="REQUIRED_PROVIDER_PATH_NOT_PROVEN",
                    reason=(
                        f"The {function} path is required for Alpha Vantage, "
                        "GDELT, RSS, or metadata enrichment but no intercepted "
                        "httpx callsite was found."
                    ),
                )
            )
        )

    return {
        "pass": not violations,
        "targets": files,
        "intercepted_surfaces": sorted(
            {
                observer
                for item in network_paths
                for observer in item["observed_by"]
            }
        ),
        "required_provider_functions": sorted(required_provider_functions),
        "observed_provider_functions": sorted(observed_provider_functions),
        "network_callsite_functions": sorted(network_callsite_functions),
        "network_paths": network_paths,
        "local_only_paths": local_only_paths,
        "violations": violations,
    }


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest().upper()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail-closed static audit of the PR #29 news network surface."
    )
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()

    report = analyze_news_network_surface(arguments.repo_root.resolve())
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    if not report["pass"]:
        for violation in report["violations"]:
            print(
                "NETWORK_SURFACE_FAIL "
                f"library={violation['library']} "
                f"module={violation['module']} "
                f"callable={violation['callable']} "
                f"file={violation['file']} "
                f"function={violation['function']} "
                f"line={violation['line']} "
                f"reason_code={violation['reason_code']} "
                f"reason={violation['reason']}"
            )
        return 1
    print(
        "NETWORK_SURFACE_PASS "
        f"observed_network_calls={len(report['network_paths'])} "
        f"local_only_calls={len(report['local_only_paths'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
