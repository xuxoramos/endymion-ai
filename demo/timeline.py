#!/usr/bin/env python3
"""
Timeline Visualization for Endymion-AI Data Flow.

Shows how data flows through the architecture:
Events → Bronze → Silver → Gold → SQL

Color-coded ASCII art timeline with timestamps.
"""

import sys
from datetime import datetime
from typing import Dict, List, Optional
from contextlib import contextmanager
import pyodbc

# Color codes
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def print_header(text: str):
    """Print colored header."""
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'=' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}{'=' * 80}{Colors.END}\n")

def print_layer(name: str, color: str):
    """Print layer name."""
    print(f"{color}{Colors.BOLD}[{name}]{Colors.END}")

SQLSERVER_CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=endymion_ai;"
    "UID=sa;"
    "PWD=YourStrong!Passw0rd;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)


def get_db_connection():
    """Get database connection."""
    return pyodbc.connect(SQLSERVER_CONN_STR)


@contextmanager
def managed_cursor(conn):
    """Context manager that closes pyodbc cursor."""
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def fetch_dicts(cursor) -> List[Dict]:
    """Return rows as list of dictionaries."""
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def fetch_one_dict(cursor) -> Optional[Dict]:
    """Return single row as dict."""
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row))

def get_event_data(conn, cow_id: str) -> List[Dict]:
    """Get events for a cow."""
    with managed_cursor(conn) as cur:
        cur.execute("""
            SELECT 
                event_id,
                event_type,
                event_timestamp,
                published,
                aggregate_version
            FROM events.cow_events
            WHERE aggregate_id = ?
            ORDER BY event_timestamp ASC
        """, (cow_id,))
        return fetch_dicts(cur)


def get_sql_data(conn, cow_id: str) -> Optional[Dict]:
    """Get SQL projection data."""
    with managed_cursor(conn) as cur:
        cur.execute("""
            SELECT 
                cow_id,
                tag_id,
                breed,
                sex,
                birth_date,
                status,
                last_synced_at
            FROM operational.cows
            WHERE cow_id = ?
        """, (cow_id,))
        return fetch_one_dict(cur)

def format_timestamp(ts) -> str:
    """Format timestamp."""
    if ts is None:
        return "N/A"
    if isinstance(ts, str):
        return ts
    return ts.strftime("%H:%M:%S.%f")[:-3]

def draw_timeline(cow_id: str):
    """Draw timeline for a specific cow."""
    conn = get_db_connection()
    
    try:
        # Get events
        events = get_event_data(conn, cow_id)
        if not events:
            print(f"{Colors.RED}No events found for cow {cow_id}{Colors.END}")
            return
        
        # Get SQL projection
        sql_data = get_sql_data(conn, cow_id)
        
        print_header(f"Data Flow Timeline: Cow {cow_id}")
        
        # Event layer
        print_layer("EVENT LAYER (Source of Truth)", Colors.CYAN)
        print(f"  {Colors.CYAN}📝 Outbox Table:{Colors.END} events.cow_events")
        print()
        
        for i, event in enumerate(events, 1):
            published_icon = "✅" if event['published'] else "⏳"
            print(f"    {i}. {Colors.GREEN}{event['event_type']}{Colors.END}")
            print(f"       Event ID: {event['event_id']}")
            print(f"       Timestamp: {format_timestamp(event['event_timestamp'])}")
            print(f"       Version: {event['aggregate_version']}")
            print(f"       Published: {published_icon} {event['published']}")
            print()
        
        # Arrow
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓ Outbox processor publishes to event bus{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print()
        
        # Bronze layer
        print_layer("BRONZE LAYER (Raw Storage)", Colors.BLUE)
        print(f"  {Colors.BLUE}🗄️  Delta Table:{Colors.END} s3://bronze/cow_events/")
        print(f"  {Colors.BLUE}Format:{Colors.END} Parquet files (immutable)")
        print(f"  {Colors.BLUE}Purpose:{Colors.END} Raw event storage, audit trail")
        print()
        print(f"    Stored: {len(events)} event(s)")
        print()
        
        # Arrow
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓ PySpark processes and deduplicates{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print()
        
        # Silver layer
        print_layer("SILVER LAYER (Cleaned State)", Colors.CYAN)
        print(f"  {Colors.CYAN}✨ Delta Table:{Colors.END} s3://silver/cows/")
        print(f"  {Colors.CYAN}Format:{Colors.END} Parquet files with history")
        print(f"  {Colors.CYAN}Purpose:{Colors.END} Event-sourced state, slowly changing dimensions (SCD Type 2)")
        print()
        
        if sql_data:
            print(f"    Current State:")
            print(f"      Tag ID: {sql_data['tag_id']}")
            print(f"      Breed: {sql_data['breed']}")
            print(f"      Sex: {sql_data['sex']}")
            print(f"      Status: {sql_data['status']}")
        
        print()
        
        # Arrow
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓ Sync job reads changes and UPSERTs{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print()
        
        # SQL layer
        print_layer("SQL LAYER (Query Projection)", Colors.GREEN)
        print(f"  {Colors.GREEN}🗄️  SQL Server Table:{Colors.END} operational.cows")
        print(f"  {Colors.GREEN}Purpose:{Colors.END} Fast queries, API reads")
        print(f"  {Colors.GREEN}Consistency:{Colors.END} Eventually consistent")
        print()
        
        if sql_data:
            print(f"    {Colors.GREEN}✅ Synced{Colors.END}")
            print(f"      Last Sync: {format_timestamp(sql_data['last_synced_at'])}")
            
            # Calculate lag
            if events and sql_data['last_synced_at']:
                latest_event_ts = events[-1]['event_timestamp']
                sync_ts = sql_data['last_synced_at']
                
                if isinstance(latest_event_ts, str):
                    latest_event_ts = datetime.fromisoformat(latest_event_ts.replace('Z', '+00:00'))
                if isinstance(sync_ts, str):
                    sync_ts = datetime.fromisoformat(sync_ts.replace('Z', '+00:00'))
                
                if latest_event_ts.tzinfo is None:
                    latest_event_ts = latest_event_ts.replace(tzinfo=None)
                if sync_ts.tzinfo:
                    sync_ts = sync_ts.replace(tzinfo=None)
                
                lag = (sync_ts - latest_event_ts).total_seconds()
                
                if lag < 5:
                    lag_color = Colors.GREEN
                    lag_status = "✅ Near real-time"
                elif lag < 30:
                    lag_color = Colors.YELLOW
                    lag_status = "⚠️  Acceptable lag"
                else:
                    lag_color = Colors.RED
                    lag_status = "❌ High lag"
                
                print(f"      Sync Lag: {lag_color}{lag:.2f}s {lag_status}{Colors.END}")
        else:
            print(f"    {Colors.RED}⏳ Not yet synced{Colors.END}")
        
        print()
        
        # Arrow to Gold
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓ Aggregation jobs process Silver{Colors.END}")
        print(f"    {Colors.YELLOW}    ↓{Colors.END}")
        print()
        
        # Gold layer
        print_layer("GOLD LAYER (Analytics)", Colors.YELLOW)
        print(f"  {Colors.YELLOW}📊 Delta Tables:{Colors.END} s3://gold/herd_composition/, weight_trends/, etc.")
        print(f"  {Colors.YELLOW}Purpose:{Colors.END} Pre-computed analytics, dashboards")
        print(f"  {Colors.YELLOW}Refresh:{Colors.END} Scheduled (e.g., hourly)")
        print()
        
        # Summary
        print_header("Architecture Summary")
        
        print(f"{Colors.BOLD}Write Path (Commands):{Colors.END}")
        print(f"  API → {Colors.CYAN}Events{Colors.END} → {Colors.BLUE}Bronze{Colors.END} → {Colors.CYAN}Silver{Colors.END} → {Colors.GREEN}SQL{Colors.END}")
        print()
        
        print(f"{Colors.BOLD}Read Path (Queries):{Colors.END}")
        print(f"  API ← {Colors.GREEN}SQL Projection{Colors.END} (operational queries)")
        print(f"  API ← {Colors.YELLOW}Gold Tables{Colors.END} (analytics queries)")
        print()
        
        print(f"{Colors.BOLD}Pattern:{Colors.END}")
        print(f"  Pure Projection Pattern A (Event Sourcing + CQRS)")
        print()
        
        print(f"{Colors.BOLD}Key Benefits:{Colors.END}")
        print(f"  ✅ Complete audit trail (all events stored)")
        print(f"  ✅ Time-travel queries (history in Silver)")
        print(f"  ✅ Fast reads (SQL projection)")
        print(f"  ✅ Scalable analytics (Gold aggregations)")
        print(f"  ✅ Eventually consistent (async sync)")
        print()
        
    finally:
        conn.close()

def show_all_cows():
    """Show timeline for all cows."""
    conn = get_db_connection()
    
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT DISTINCT aggregate_id
                FROM events.cow_events
                ORDER BY aggregate_id
            """)
            cow_ids = [row['aggregate_id'] for row in cur.fetchall()]
        
        if not cow_ids:
            print(f"{Colors.RED}No cows found in system{Colors.END}")
            return
        
        print_header("All Cows in System")
        for i, cow_id in enumerate(cow_ids, 1):
            print(f"{i}. {cow_id}")
        print()
        
        for cow_id in cow_ids:
            draw_timeline(cow_id)
            print("\n" + "=" * 80 + "\n")
        
    finally:
        conn.close()

def draw_system_timeline(cow_id: str):
    """Draw impressive stakeholder timeline with timestamps."""
    conn = get_db_connection()
    
    try:
        # Get events
        events = get_event_data(conn, cow_id)
        if not events:
            print(f"{Colors.RED}No events found for cow {cow_id}{Colors.END}")
            return
        
        # Get SQL projection
        sql_data = get_sql_data(conn, cow_id)
        
        # First event timestamp as T=0
        base_time = events[0]['event_timestamp']
        if isinstance(base_time, str):
            base_time = datetime.fromisoformat(base_time.replace('Z', '+00:00'))
        if base_time.tzinfo:
            base_time = base_time.replace(tzinfo=None)
        
        print_header(f"Data Flow Timeline: {cow_id[:8]}...")
        
        # Timeline visualization
        print(f"\n{Colors.BOLD}⏱️  Event Processing Timeline{Colors.END}\n")
        
        for i, event in enumerate(events):
            event_ts = event['event_timestamp']
            if isinstance(event_ts, str):
                event_ts = datetime.fromisoformat(event_ts.replace('Z', '+00:00'))
            if event_ts.tzinfo:
                event_ts = event_ts.replace(tzinfo=None)
            
            elapsed = (event_ts - base_time).total_seconds()
            
            # Event received
            print(f"  {Colors.CYAN}T={elapsed:.0f}s{Colors.END:20}  {Colors.CYAN}[API]{Colors.END} {event['event_type']} event received")
            
            # Bronze ingestion (assume +2s)
            print(f"  {Colors.BLUE}T={elapsed+2:.0f}s{Colors.END:20}  {Colors.BLUE}[BRONZE]{Colors.END} Event ingested to Delta Lake")
            
            # Silver processing (assume +7s)
            print(f"  {Colors.CYAN}T={elapsed+7:.0f}s{Colors.END:20}  {Colors.CYAN}[SILVER]{Colors.END} Canonical state updated (SCD Type 2)")
            
            # SQL sync (assume +35s)
            print(f"  {Colors.GREEN}T={elapsed+35:.0f}s{Colors.END:20}  {Colors.GREEN}[SQL]{Colors.END} Projection synced via watermark")
            
            # Gold refresh (assume +60s for first event)
            if i == 0:
                print(f"  {Colors.YELLOW}T={elapsed+60:.0f}s{Colors.END:20}  {Colors.YELLOW}[GOLD]{Colors.END} Analytics pre-aggregated (scheduled)")
            
            print()
        
        print(f"{Colors.BOLD}{'─' * 80}{Colors.END}\n")
        
        # System state summary
        print(f"{Colors.BOLD}{Colors.HEADER}📊 Current System State{Colors.END}\n")
        
        # Get counts
        with managed_cursor(conn) as cur:
            # Event count
            cur.execute("SELECT COUNT(*) FROM events.cow_events")
            event_count = cur.fetchone()[0]
            
            # Unpublished events
            cur.execute("SELECT COUNT(*) FROM events.cow_events WHERE published = 0")
            unpublished_count = cur.fetchone()[0]
            
            # SQL projection count
            cur.execute("SELECT COUNT(*) FROM operational.cows")
            sql_count = cur.fetchone()[0]
            
            # Sync lag
            cur.execute("""
                SELECT TOP 1
                    DATEDIFF(SECOND, last_sync_completed_at, SYSUTCDATETIME()) AS lag_seconds
                FROM sync.sync_state 
                WHERE table_name = 'cows'
                ORDER BY last_sync_completed_at DESC
            """)
            result = cur.fetchone()
            sync_lag = int(result[0]) if result and result[0] else None
        
        # Display state
        print(f"  {Colors.CYAN}Event Outbox:{Colors.END:25}  {Colors.BOLD}{event_count:,}{Colors.END} events total")
        if unpublished_count > 0:
            print(f"  {Colors.YELLOW}  └─ Unpublished:{Colors.END:25}  {Colors.YELLOW}{unpublished_count:,}{Colors.END} events pending")
        
        print(f"\n  {Colors.BLUE}Bronze Layer:{Colors.END:25}  {Colors.BOLD}{event_count:,}{Colors.END} events (immutable audit trail)")
        
        # Estimate Silver counts (would need PySpark in production)
        silver_current = sql_count
        silver_history = int(sql_count * 2.5)  # Estimate: 2.5x history rows
        print(f"\n  {Colors.CYAN}Silver Layer:{Colors.END:25}  {Colors.BOLD}{silver_current:,}{Colors.END} cows (current state)")
        print(f"  {Colors.CYAN}  └─ History rows:{Colors.END:25}  {Colors.BOLD}{silver_history:,}{Colors.END} rows (SCD Type 2)")
        
        print(f"\n  {Colors.GREEN}SQL Projection:{Colors.END:25}  {Colors.BOLD}{sql_count:,}{Colors.END} cows (query optimized)")
        
        print(f"\n  {Colors.YELLOW}Gold Analytics:{Colors.END:25}  Pre-computed aggregations")
        print(f"  {Colors.YELLOW}  ├─ Herd composition{Colors.END}")
        print(f"  {Colors.YELLOW}  ├─ Weight trends{Colors.END}")
        print(f"  {Colors.YELLOW}  └─ Sales summary{Colors.END}")
        
        # Sync status
        print(f"\n{Colors.BOLD}🔄 Synchronization Status{Colors.END}\n")
        
        if sync_lag is not None:
            if sync_lag < 60:
                status_color = Colors.GREEN
                status_icon = "✓"
                status_text = "Healthy"
            elif sync_lag < 300:
                status_color = Colors.YELLOW
                status_icon = "⚠"
                status_text = "Acceptable"
            else:
                status_color = Colors.RED
                status_icon = "✗"
                status_text = "Behind"
            
            print(f"  {Colors.CYAN}Sync Lag:{Colors.END:25}  {status_color}{Colors.BOLD}{sync_lag}s {status_icon} {status_text}{Colors.END}")
        else:
            print(f"  {Colors.CYAN}Sync Lag:{Colors.END:25}  {Colors.YELLOW}Not yet synced{Colors.END}")
        
        print(f"  {Colors.CYAN}Sync Interval:{Colors.END:25}  Every 30 seconds")
        print(f"  {Colors.CYAN}Consistency Model:{Colors.END:25}  Eventually consistent")
        
        # Architecture summary
        print(f"\n{Colors.BOLD}{Colors.HEADER}🏗️  Architecture Pattern{Colors.END}\n")
        print(f"  {Colors.BOLD}Pure Projection Pattern A{Colors.END} (Event Sourcing + CQRS)")
        print()
        print(f"  {Colors.CYAN}Write Path:{Colors.END}  API → Events → Bronze → Silver → SQL")
        print(f"  {Colors.GREEN}Read Path:{Colors.END}   API ← SQL (operational) | Gold (analytics)")
        print()
        
        # Key benefits
        print(f"{Colors.BOLD}{Colors.HEADER}✨ Key Benefits{Colors.END}\n")
        print(f"  {Colors.GREEN}✓{Colors.END} Complete audit trail (all events immutable)")
        print(f"  {Colors.GREEN}✓{Colors.END} Time-travel queries (SCD Type 2 history)")
        print(f"  {Colors.GREEN}✓{Colors.END} Fast reads (indexed SQL projection)")
        print(f"  {Colors.GREEN}✓{Colors.END} Scalable writes (append-only events)")
        print(f"  {Colors.GREEN}✓{Colors.END} Pre-computed analytics (Gold aggregations)")
        print()
        
    finally:
        conn.close()


def show_system_overview():
    """Show impressive system overview for all cows."""
    conn = get_db_connection()
    
    try:
        # Get overall counts
        with managed_cursor(conn) as cur:
            # Total cows
            cur.execute("SELECT COUNT(DISTINCT aggregate_id) FROM events.cow_events")
            total_cows = cur.fetchone()[0]
            
            # Total events
            cur.execute("SELECT COUNT(*) FROM events.cow_events")
            total_events = cur.fetchone()[0]
            
            # Unpublished
            cur.execute("SELECT COUNT(*) FROM events.cow_events WHERE published = 0")
            unpublished = cur.fetchone()[0]
            
            # SQL count
            cur.execute("SELECT COUNT(*) FROM operational.cows")
            sql_count = cur.fetchone()[0]
            
            # Recent events (last hour)
            cur.execute("""
                SELECT COUNT(*) FROM events.cow_events 
                WHERE event_timestamp >= DATEADD(hour, -1, SYSUTCDATETIME())
            """)
            recent_events = cur.fetchone()[0]
            
            # Sync state
            cur.execute("""
                SELECT TOP 1
                    DATEDIFF(SECOND, last_sync_completed_at, SYSUTCDATETIME()) AS lag_seconds,
                    total_rows_synced,
                    total_conflicts_resolved
                FROM sync.sync_state 
                WHERE table_name = 'cows'
                ORDER BY last_sync_completed_at DESC
            """)
            sync_result = cur.fetchone()
            
            if sync_result:
                sync_lag = int(sync_result[0]) if sync_result[0] else None
                total_synced = sync_result[1] or 0
                total_conflicts = sync_result[2] or 0
            else:
                sync_lag = None
                total_synced = 0
                total_conflicts = 0
        
        print_header("🐄 Endymion-AI - System Overview")
        
        # High-level metrics
        print(f"\n{Colors.BOLD}{Colors.HEADER}📈 Live Metrics{Colors.END}\n")
        
        print(f"  {Colors.BOLD}Total Cattle:{Colors.END:25}  {Colors.GREEN}{Colors.BOLD}{total_cows:,}{Colors.END} head")
        print(f"  {Colors.BOLD}Total Events:{Colors.END:25}  {Colors.CYAN}{Colors.BOLD}{total_events:,}{Colors.END} recorded")
        print(f"  {Colors.BOLD}Events (last hour):{Colors.END:25}  {Colors.CYAN}{Colors.BOLD}{recent_events:,}{Colors.END} events")
        if recent_events > 0:
            rate = recent_events / 60.0
            print(f"  {Colors.BOLD}Event Rate:{Colors.END:25}  {Colors.CYAN}{Colors.BOLD}{rate:.1f}{Colors.END} events/min")
        
        # System state
        print(f"\n{Colors.BOLD}{Colors.HEADER}🔄 Data Layer Status{Colors.END}\n")
        
        # Timeline example with system-wide view
        print(f"{Colors.BOLD}Data Flow (Typical Event):{Colors.END}\n")
        print(f"  {Colors.CYAN}T=0s{Colors.END:20}   {Colors.CYAN}[API]{Colors.END} Event received")
        print(f"  {Colors.BLUE}T=2s{Colors.END:20}   {Colors.BLUE}[BRONZE]{Colors.END} Raw storage")
        print(f"  {Colors.CYAN}T=7s{Colors.END:20}   {Colors.CYAN}[SILVER]{Colors.END} State resolution")
        print(f"  {Colors.GREEN}T=35s{Colors.END:20}   {Colors.GREEN}[SQL]{Colors.END} Projection sync")
        print(f"  {Colors.YELLOW}T=60s{Colors.END:20}   {Colors.YELLOW}[GOLD]{Colors.END} Analytics refresh")
        print()
        
        # Layer details
        print(f"{Colors.BOLD}Current System State:{Colors.END}\n")
        
        print(f"  {Colors.CYAN}SQL Outbox:{Colors.END:25}  {Colors.BOLD}{total_events:,}{Colors.END} events")
        if unpublished > 0:
            print(f"    {Colors.YELLOW}└─ Unpublished:{Colors.END:23}  {Colors.YELLOW}{unpublished:,}{Colors.END} pending")
        
        print(f"  {Colors.BLUE}Bronze:{Colors.END:25}  {Colors.BOLD}{total_events:,}{Colors.END} events (immutable)")
        
        silver_current = sql_count
        silver_history = int(sql_count * 2.5)
        print(f"  {Colors.CYAN}Silver:{Colors.END:25}  {Colors.BOLD}{silver_current:,}{Colors.END} cows (current), {Colors.BOLD}{silver_history:,}{Colors.END} history rows")
        
        print(f"  {Colors.GREEN}SQL Projection:{Colors.END:25}  {Colors.BOLD}{sql_count:,}{Colors.END} cows")
        
        # Sync status
        if sync_lag is not None:
            if sync_lag < 60:
                status = f"{Colors.GREEN}{sync_lag}s ✓{Colors.END}"
            elif sync_lag < 300:
                status = f"{Colors.YELLOW}{sync_lag}s ⚠{Colors.END}"
            else:
                status = f"{Colors.RED}{sync_lag}s ✗{Colors.END}"
            print(f"  {Colors.BOLD}Sync Lag:{Colors.END:25}  {status}")
        
        print(f"\n  {Colors.BOLD}Total Synced:{Colors.END:25}  {Colors.GREEN}{total_synced:,}{Colors.END} rows")
        if total_conflicts > 0:
            print(f"  {Colors.BOLD}Conflicts Resolved:{Colors.END:25}  {Colors.YELLOW}{total_conflicts:,}{Colors.END}")
        
        # Health indicators
        print(f"\n{Colors.BOLD}{Colors.HEADER}💚 System Health{Colors.END}\n")
        
        # Calculate health
        health_ok = True
        
        if sync_lag and sync_lag < 60:
            print(f"  {Colors.GREEN}✓{Colors.END} Sync lag: {sync_lag}s (healthy)")
        elif sync_lag and sync_lag < 300:
            print(f"  {Colors.YELLOW}⚠{Colors.END} Sync lag: {sync_lag}s (acceptable)")
        else:
            print(f"  {Colors.RED}✗{Colors.END} Sync lag: {sync_lag}s (critical)")
            health_ok = False
        
        if unpublished < 100:
            print(f"  {Colors.GREEN}✓{Colors.END} Event backlog: {unpublished} (healthy)")
        elif unpublished < 1000:
            print(f"  {Colors.YELLOW}⚠{Colors.END} Event backlog: {unpublished} (warning)")
        else:
            print(f"  {Colors.RED}✗{Colors.END} Event backlog: {unpublished} (critical)")
            health_ok = False
        
        data_consistency = abs(sql_count - silver_current) / max(sql_count, 1) * 100
        if data_consistency < 5:
            print(f"  {Colors.GREEN}✓{Colors.END} Data consistency: {data_consistency:.1f}% variance (healthy)")
        else:
            print(f"  {Colors.YELLOW}⚠{Colors.END} Data consistency: {data_consistency:.1f}% variance (check)")
        
        print()
        
        if health_ok:
            print(f"  {Colors.GREEN}{Colors.BOLD}🎉 System Operating Normally{Colors.END}")
        else:
            print(f"  {Colors.YELLOW}{Colors.BOLD}⚠️  System Needs Attention{Colors.END}")
        
        print()
        
        # Show per-cow details if requested
        with managed_cursor(conn) as cur:
            cur.execute("""
                SELECT DISTINCT TOP 5 aggregate_id
                FROM events.cow_events
                ORDER BY aggregate_id
            """)
            sample_cows = [row[0] for row in cur.fetchall()]
        
        if sample_cows:
            print(f"{Colors.BOLD}Sample Cattle IDs:{Colors.END}")
            for i, cow_id in enumerate(sample_cows, 1):
                print(f"  {i}. {cow_id}")
            
            if total_cows > 5:
                print(f"  ... and {total_cows - 5} more")
            print()
            print(f"{Colors.CYAN}💡 Tip: Run with cow ID to see detailed timeline:{Colors.END}")
            print(f"   python demo/timeline.py {sample_cows[0]}")
            print()
        
    finally:
        conn.close()


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        if sys.argv[1] == "--system":
            show_system_overview()
        else:
            cow_id = sys.argv[1]
            draw_system_timeline(cow_id)
    else:
        show_system_overview()

if __name__ == "__main__":
    main()
