"""Tests for entry point scoring logic used in flow detection.

Pure-function tests — no database required.
"""

import re
import pytest


# Import scoring constants and functions directly
# These are defined in scripts/detect_flows.py
ENTRY_PATTERNS = {
    '*': [r'^(main|init|bootstrap|start|run|setup)$', r'^handle[A-Z]', r'^on[A-Z]',
          r'Handler$', r'Controller$', r'^process[A-Z]', r'^execute[A-Z]', r'^dispatch[A-Z]'],
    'python': [r'^(get|post|put|delete)_', r'^api_', r'^view_', r'^app$'],
    'typescript': [r'^use[A-Z]'],
    'go': [r'Handler$', r'^Serve', r'^New[A-Z]', r'^Make[A-Z]',
           r'^Msg', r'^Query', r'^BeginBlocker$', r'^EndBlocker$',
           r'^InitGenesis$', r'^ExportGenesis$',
           r'^Keeper\.', r'^NewKeeper$', r'^NewMsgServer', r'^NewQueryServer'],
}

UTILITY_PATTERNS = [r'^(get|set|is|has|can|should)[A-Z]', r'^_',
                    r'^(format|parse|validate|convert|transform)',
                    r'^(log|debug|error|warn|info)$',
                    r'^(to|from)[A-Z]', r'^(encode|decode)', r'Helper$', r'Util$']

FRAMEWORK_PATH_PATTERNS = {
    'fastapi': (['/routers/', '/endpoints/', '/api/'], '.py', 2.5),
    'express': (['/routes/'], '.ts', 2.5),
    'go-http': (['/handlers/', '/handler/'], '.go', 2.5),
    'cosmos-keeper': (['/keeper/'], '.go', 3.0),
    'cosmos-cli': (['/cli/'], '.go', 2.5),
    'cosmos-module': (['/module/'], '.go', 2.0),
    'nextjs-pages': (['/pages/'], '.tsx', 3.0),
    'nextjs-api': (['/pages/api/', '/app/api/'], '.ts', 3.0),
}

TEST_PATTERNS = ['/test/', '/tests/', '.test.', '.spec.', '_test.go', '_test.py',
                 'test_', '__tests__/']


def matches_patterns(name: str, patterns: list) -> bool:
    """Check if name matches any regex pattern."""
    return any(re.search(p, name) for p in patterns)


def get_name_multiplier(name: str, language: str) -> float:
    """Get entry point name multiplier."""
    # Check utility patterns first (penalty)
    if matches_patterns(name, UTILITY_PATTERNS):
        return 0.3
    # Check language-specific entry patterns
    lang_patterns = ENTRY_PATTERNS.get(language, [])
    universal_patterns = ENTRY_PATTERNS.get('*', [])
    if matches_patterns(name, lang_patterns) or matches_patterns(name, universal_patterns):
        return 1.5
    return 1.0


def get_framework_multiplier(file_path: str) -> float:
    """Get framework path multiplier."""
    for framework, (paths, ext, multiplier) in FRAMEWORK_PATH_PATTERNS.items():
        if file_path.endswith(ext) and any(p in file_path for p in paths):
            return multiplier
    return 1.0


def is_test_file(file_path: str) -> bool:
    """Check if file is a test file."""
    return any(p in file_path for p in TEST_PATTERNS)


def score_entry_point(name: str, language: str, file_path: str,
                      callee_count: int, caller_count: int,
                      is_exported: bool = False) -> float:
    """Calculate entry point score."""
    base = callee_count / (caller_count + 1)
    export_mult = 2.0 if is_exported else 1.0
    name_mult = get_name_multiplier(name, language)
    framework_mult = get_framework_multiplier(file_path)
    return base * export_mult * name_mult * framework_mult


# --- Tests ---

class TestEntryPointPatterns:
    """Test entry point name pattern matching."""

    def test_handler_function_matches(self):
        assert matches_patterns('handleLogin', ENTRY_PATTERNS['*'])
        assert matches_patterns('handleRequest', ENTRY_PATTERNS['*'])

    def test_on_event_matches(self):
        assert matches_patterns('onClick', ENTRY_PATTERNS['*'])
        assert matches_patterns('onSubmit', ENTRY_PATTERNS['*'])

    def test_controller_suffix_matches(self):
        assert matches_patterns('UserController', ENTRY_PATTERNS['*'])
        assert matches_patterns('AuthHandler', ENTRY_PATTERNS['*'])

    def test_main_function_matches(self):
        assert matches_patterns('main', ENTRY_PATTERNS['*'])
        assert matches_patterns('init', ENTRY_PATTERNS['*'])
        assert matches_patterns('setup', ENTRY_PATTERNS['*'])

    def test_process_execute_matches(self):
        assert matches_patterns('processEvent', ENTRY_PATTERNS['*'])
        assert matches_patterns('executeQuery', ENTRY_PATTERNS['*'])
        assert matches_patterns('dispatchAction', ENTRY_PATTERNS['*'])

    def test_regular_function_no_match(self):
        assert not matches_patterns('calculateTotal', ENTRY_PATTERNS['*'])
        assert not matches_patterns('renderComponent', ENTRY_PATTERNS['*'])


class TestCosmosSDKPatterns:
    """Test Cosmos SDK-specific entry point patterns."""

    def test_msg_patterns(self):
        assert matches_patterns('MsgCreateBatch', ENTRY_PATTERNS['go'])
        assert matches_patterns('MsgSend', ENTRY_PATTERNS['go'])
        assert matches_patterns('MsgRetire', ENTRY_PATTERNS['go'])

    def test_query_patterns(self):
        assert matches_patterns('QueryBalance', ENTRY_PATTERNS['go'])
        assert matches_patterns('QuerySupply', ENTRY_PATTERNS['go'])

    def test_keeper_patterns(self):
        assert matches_patterns('Keeper.Create', ENTRY_PATTERNS['go'])
        assert matches_patterns('NewKeeper', ENTRY_PATTERNS['go'])

    def test_genesis_patterns(self):
        assert matches_patterns('BeginBlocker', ENTRY_PATTERNS['go'])
        assert matches_patterns('EndBlocker', ENTRY_PATTERNS['go'])
        assert matches_patterns('InitGenesis', ENTRY_PATTERNS['go'])
        assert matches_patterns('ExportGenesis', ENTRY_PATTERNS['go'])

    def test_server_patterns(self):
        assert matches_patterns('NewMsgServer', ENTRY_PATTERNS['go'])
        assert matches_patterns('NewQueryServer', ENTRY_PATTERNS['go'])
        assert matches_patterns('ServeHTTP', ENTRY_PATTERNS['go'])


class TestUtilityPatterns:
    """Test utility function penalty patterns."""

    def test_getter_setter(self):
        assert matches_patterns('getName', UTILITY_PATTERNS)
        assert matches_patterns('setConfig', UTILITY_PATTERNS)
        assert matches_patterns('isValid', UTILITY_PATTERNS)
        assert matches_patterns('hasPermission', UTILITY_PATTERNS)

    def test_private_underscore(self):
        assert matches_patterns('_internal', UTILITY_PATTERNS)
        assert matches_patterns('_helper', UTILITY_PATTERNS)

    def test_format_parse(self):
        assert matches_patterns('formatDate', UTILITY_PATTERNS)
        assert matches_patterns('parseJSON', UTILITY_PATTERNS)
        assert matches_patterns('validateInput', UTILITY_PATTERNS)

    def test_conversion(self):
        assert matches_patterns('toString', UTILITY_PATTERNS)
        assert matches_patterns('fromBytes', UTILITY_PATTERNS)
        assert matches_patterns('encodeBinary', UTILITY_PATTERNS)
        assert matches_patterns('decodeHex', UTILITY_PATTERNS)

    def test_helper_util_suffix(self):
        assert matches_patterns('authHelper', UTILITY_PATTERNS)
        assert matches_patterns('stringUtil', UTILITY_PATTERNS)

    def test_log_functions(self):
        assert matches_patterns('log', UTILITY_PATTERNS)
        assert matches_patterns('debug', UTILITY_PATTERNS)
        assert matches_patterns('error', UTILITY_PATTERNS)


class TestFrameworkPathDetection:
    """Test framework-specific path multiplier."""

    def test_fastapi_routes(self):
        assert get_framework_multiplier('/routers/auth.py') == 2.5
        assert get_framework_multiplier('/api/endpoints.py') == 2.5

    def test_express_routes(self):
        assert get_framework_multiplier('/routes/users.ts') == 2.5

    def test_cosmos_keeper(self):
        assert get_framework_multiplier('/keeper/msg_server.go') == 3.0

    def test_cosmos_cli(self):
        assert get_framework_multiplier('/cli/tx.go') == 2.5

    def test_nextjs_api(self):
        assert get_framework_multiplier('/pages/api/auth.ts') == 3.0
        assert get_framework_multiplier('/app/api/route.ts') == 3.0

    def test_regular_path_no_boost(self):
        assert get_framework_multiplier('/src/utils/helper.py') == 1.0
        assert get_framework_multiplier('/internal/service.go') == 1.0


class TestScoringFormula:
    """Test the entry point scoring formula."""

    def test_handler_high_score(self):
        # Handler function: calls many, called by few, exported
        score = score_entry_point('handleLogin', 'python', '/api/auth.py',
                                   callee_count=10, caller_count=1,
                                   is_exported=True)
        # (10/2) * 2.0 * 1.5 * 2.5 = 37.5
        assert score == pytest.approx(37.5)

    def test_utility_low_score(self):
        # Utility: called by many, calls few
        score = score_entry_point('getName', 'python', '/utils/helpers.py',
                                   callee_count=0, caller_count=20,
                                   is_exported=False)
        # (0/21) * 1.0 * 0.3 * 1.0 = 0.0
        assert score == 0.0

    def test_private_utility_penalty(self):
        # Private function gets utility penalty
        score = score_entry_point('_setupDB', 'python', '/db/init.py',
                                   callee_count=5, caller_count=1,
                                   is_exported=False)
        # (5/2) * 1.0 * 0.3 * 1.0 = 0.75
        assert score == pytest.approx(0.75)

    def test_cosmos_keeper_method_high_score(self):
        # Cosmos keeper method in keeper directory
        score = score_entry_point('NewKeeper', 'go', '/keeper/keeper.go',
                                   callee_count=8, caller_count=0,
                                   is_exported=True)
        # (8/1) * 2.0 * 1.5 * 3.0 = 72.0
        assert score == pytest.approx(72.0)

    def test_no_callees_zero_score(self):
        # Leaf function with no outgoing calls
        score = score_entry_point('doSomething', 'python', '/src/app.py',
                                   callee_count=0, caller_count=5,
                                   is_exported=False)
        assert score == 0.0

    def test_exported_multiplier(self):
        score_private = score_entry_point('run', 'python', '/src/main.py',
                                           callee_count=5, caller_count=0,
                                           is_exported=False)
        score_exported = score_entry_point('run', 'python', '/src/main.py',
                                            callee_count=5, caller_count=0,
                                            is_exported=True)
        assert score_exported == score_private * 2.0


class TestTestFileDetection:
    """Test file detection for filtering."""

    def test_python_test_file(self):
        assert is_test_file('/tests/test_auth.py')
        assert is_test_file('test_helpers.py')
        assert is_test_file('/test/unit/test_db.py')

    def test_go_test_file(self):
        assert is_test_file('/keeper/keeper_test.go')

    def test_js_test_file(self):
        assert is_test_file('/src/auth.test.ts')
        assert is_test_file('/src/auth.spec.js')
        assert is_test_file('/__tests__/utils.ts')

    def test_regular_file_not_test(self):
        assert not is_test_file('/src/auth.py')
        assert not is_test_file('/keeper/keeper.go')
        assert not is_test_file('/api/routes.ts')
