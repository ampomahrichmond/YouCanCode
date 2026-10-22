"""
diagnose.py
===========
Run this FIRST to verify your API credentials before running the bot.

Usage:
    python diagnose.py              # Tests demo mode (default)
    python diagnose.py --live       # Tests live mode

Author: APEX Trading Suite - Kalshi Module
"""

import os
import sys
import argparse
from pathlib import Path


def find_and_load_env() -> Path:
    """
    Robustly find and load the .env file on Windows, Mac, or Linux.
    Searches: script directory → current working directory → parent dirs.
    Returns the Path to the .env that was loaded, or None.
    """
    from dotenv import load_dotenv, dotenv_values

    # Candidates in priority order
    script_dir = Path(__file__).resolve().parent
    cwd        = Path.cwd()

    candidates = [
        script_dir / ".env",          # same folder as diagnose.py  ← most reliable on Windows
        cwd        / ".env",          # wherever you ran the script from
        script_dir.parent / ".env",   # one level up
    ]

    for path in candidates:
        if path.exists():
            load_dotenv(dotenv_path=str(path), override=True)
            return path

    # Last resort: let dotenv search upward automatically
    load_dotenv(override=True)
    return None


def run_diagnostics(mode: str = "demo"):
    print("""
╔══════════════════════════════════════════════════════╗
║   🔍  KALSHI CONNECTION DIAGNOSTICS                  ║
║   APEX Trading Suite — v2                           ║
╚══════════════════════════════════════════════════════╝
""")

    # ── Step 1: Find and load .env ───────────────────────────────────────────
    print("[ Step 1 ] Locating .env file...")

    env_path = find_and_load_env()

    script_dir = Path(__file__).resolve().parent
    env_template = script_dir / ".env.template"

    if env_path and env_path.exists():
        print(f"  ✅ .env found: {env_path}")
    else:
        print(f"  ❌ No .env file found!")
        print(f"\n  Looked in:")
        print(f"    • {script_dir / '.env'}   ← put it here (same folder as diagnose.py)")
        print(f"    • {Path.cwd() / '.env'}")
        print(f"\n  To fix:")
        if env_template.exists():
            print(f"    1. In File Explorer, go to: {script_dir}")
            print(f"    2. Copy  .env.template  →  .env")
            print(f"    3. Open .env in Notepad and fill in your API key\n")
        else:
            print(f"    Create a file called .env in: {script_dir}")
            print(f"    With these two lines:\n")
            print(f"      KALSHI_API_KEY_ID=your_key_id_here")
            print(f"      KALSHI_PRIVATE_KEY_PATH=./kalshi_private_key.pem\n")
        sys.exit(1)

    # ── Show .env contents (redacted) ────────────────────────────────────────
    print(f"\n  .env contents (redacted):")
    try:
        raw_lines = env_path.read_text(encoding="utf-8").splitlines()
        for line in raw_lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                # Redact secrets
                if any(secret in key.upper() for secret in ["TOKEN","KEY","SECRET","PASS","PWD"]):
                    display = val[:6] + "..." if len(val) > 6 else "(set)"
                else:
                    display = val
                status = "✅" if val else "❌ EMPTY"
                print(f"    {status}  {key} = {display}")
    except Exception as e:
        print(f"    ⚠️  Could not read .env: {e}")

    # ── Step 2: Check key ID ─────────────────────────────────────────────────
    print("\n[ Step 2 ] Checking KALSHI_API_KEY_ID...")
    key_id = os.getenv("KALSHI_API_KEY_ID", "").strip().strip('"').strip("'")

    if not key_id:
        print("  ❌ KALSHI_API_KEY_ID is empty")
        print("\n  Common causes on Windows:")
        print("  1. .env has quotes around the value:  KALSHI_API_KEY_ID=\"abc123\"")
        print("     → Remove the quotes:  KALSHI_API_KEY_ID=abc123")
        print("  2. .env was saved as .env.txt by Notepad")
        print("     → In File Explorer: View → Show → File name extensions")
        print("     → Rename .env.txt to .env (remove the .txt)")
        print("  3. Value has leading/trailing spaces")
        print("     → Check your .env: the line should be exactly:")
        print("        KALSHI_API_KEY_ID=yourKeyIdHere  (no spaces around =)\n")
        sys.exit(1)

    print(f"  ✅ Key ID: {key_id[:14]}{'...' if len(key_id) > 14 else ''}")

    # ── Step 3: Check private key file ───────────────────────────────────────
    print("\n[ Step 3 ] Checking private key file...")
    raw_key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "./kalshi_private_key.pem").strip().strip('"').strip("'")

    # Resolve relative to script dir (not CWD — more reliable on Windows)
    key_path = Path(raw_key_path)
    if not key_path.is_absolute():
        key_path = (script_dir / key_path).resolve()

    print(f"  Path configured: {raw_key_path}")
    print(f"  Resolved to:     {key_path}")

    if not key_path.exists():
        print(f"\n  ❌ PEM file not found at: {key_path}")
        print(f"\n  To fix:")
        print(f"  1. Log in to demo.kalshi.co → Settings → API")
        print(f"  2. Create a new API key → Download the .pem file")
        print(f"  3. Copy the .pem file to: {script_dir}")
        print(f"  4. Rename it to: kalshi_private_key.pem")
        print(f"  5. Or update KALSHI_PRIVATE_KEY_PATH in .env to the actual path\n")
        sys.exit(1)

    pem_data = key_path.read_bytes()
    if b"PRIVATE KEY" not in pem_data:
        print(f"  ❌ File exists but doesn't look like a PEM private key")
        print(f"     First bytes: {pem_data[:60]}")
        print(f"     → Re-download the .pem file from Kalshi → Settings → API")
        sys.exit(1)

    print(f"  ✅ PEM file found ({len(pem_data)} bytes)")

    # ── Step 4: Parse RSA key ─────────────────────────────────────────────────
    print("\n[ Step 4 ] Parsing RSA private key...")
    try:
        from cryptography.hazmat.primitives import serialization
        pk = serialization.load_pem_private_key(pem_data, password=None)
        print(f"  ✅ RSA-{pk.key_size} private key loaded")
    except ImportError:
        print("  ❌ 'cryptography' library missing")
        print("     → Run:  pip install cryptography")
        sys.exit(1)
    except Exception as e:
        print(f"  ❌ Key parse failed: {e}")
        print("     → Re-download the .pem file from Kalshi → Settings → API")
        sys.exit(1)

    # ── Step 5: API connection ────────────────────────────────────────────────
    print(f"\n[ Step 5 ] Connecting to Kalshi {mode.upper()} API...")

    # Add parent dir to path so we can import kalshi_client
    sys.path.insert(0, str(script_dir))
    try:
        from kalshi_client import KalshiClient
    except ImportError as e:
        print(f"  ❌ Cannot import KalshiClient: {e}")
        print(f"     → Make sure all bot files are in: {script_dir}")
        sys.exit(1)

    try:
        client = KalshiClient(
            key_id=key_id,
            private_key_path=str(key_path),
            mode=mode,
        )
    except Exception as e:
        print(f"  ❌ Client init failed: {e}")
        sys.exit(1)

    d = client.diagnose()
    print(f"  Host reachable: {'✅' if d['host_reachable'] else '❌ FAILED'}")

    if not d["host_reachable"]:
        print(f"  ❌ Cannot reach {d['base_url']}")
        print("     → Check your internet connection")
        sys.exit(1)

    print(f"  Auth OK:        {'✅' if d['auth_ok'] else '❌ FAILED'}")

    if not d["auth_ok"]:
        print(f"\n  ❌ Authentication FAILED")
        print(f"  Error: {d['error']}")
        print("""
  Common causes:
  ① Key ID and PEM file don't match
     → They must be from the SAME key pair generated at the same time
     → Re-generate both together: Kalshi Settings → API → Create Key

  ② Wrong environment (demo vs live)
     → demo.kalshi.co keys only work with --mode demo
     → kalshi.com keys only work with --mode live
     → Your .env should have: KALSHI_MODE=demo (for demo keys)

  ③ System clock is wrong (RSA timestamps expire after ~30 seconds)
     → Windows: right-click clock → Adjust date/time → Sync now
     → Or run:  w32tm /resync  in an admin PowerShell

  ④ Key was just created (< 2 min ago)
     → Wait 2 minutes and try again

  ⑤ Key needs Trading permission
     → Kalshi Settings → API → Edit key → enable Trading
        """)
        sys.exit(1)

    # ── Step 6: Account details ───────────────────────────────────────────────
    print(f"\n[ Step 6 ] Fetching account details...")
    print(f"  ✅ Balance:        ${d['balance_usd']:,.2f}")

    try:
        positions = client.get_positions()
        print(f"  ✅ Open positions: {len(positions)}")
        for pos in positions[:5]:
            t = pos.get("ticker","?")
            c = pos.get("position", pos.get("contracts", "?"))
            print(f"       {t}: {c} contracts")
    except Exception as e:
        print(f"  ⚠️  Positions error: {e}")

    try:
        orders = client.get_orders(status="resting")
        print(f"  ✅ Resting orders: {len(orders)}")
    except Exception as e:
        print(f"  ⚠️  Orders error: {e}")

    # ── All good ──────────────────────────────────────────────────────────────
    print(f"""
{'═'*56}
  🎉  ALL CHECKS PASSED — Ready to trade on Kalshi {mode.upper()}!

  Next steps:
    python bot.py --mode {mode} --scan    (scan, no orders)
    python bot.py --mode {mode}           (start trading)
    python scanner.py                     (signal dashboard)
{'═'*56}
""")
    return True


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Kalshi API Connection Diagnostic")
    p.add_argument("--live", action="store_true", help="Test LIVE environment (default: demo)")
    args = p.parse_args()
    run_diagnostics("live" if args.live else "demo")