import subprocess
import sys

result = subprocess.run([sys.executable, "test_ast.py"], capture_output=True, text=True)
print(result.stdout)
print(result.stderr)
