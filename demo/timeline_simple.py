#!/usr/bin/env python
"""
Simple timeline visualization for Endymion-AI data flow (SQL Server version)
"""
import sys
sys.path.insert(0, '/home/xuxoramos/endymion-ai')

from backend.database.connection import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# Colors
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'

def main():
    db = DatabaseManager()
    
    print(f"\n{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.HEADER}Endymion-AI Data Flow Timeline{Colors.END}".center(90))
    print(f"{Colors.BOLD}{Colors.HEADER}{'='*80}{Colors.END}\n")
    
    with db.engine.connect() as conn:
        # Get event statistics
        result = conn.execute(text("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT cow_id) as unique_cows,
                COUNT(DISTINCT event_type) as event_types,
                MIN(event_time) as first_event,
                MAX(event_time) as last_event
            FROM operational.cow_events
        """))
        stats = result.fetchone()
        
        print(f"{Colors.CYAN}📊 Event Store Statistics:{Colors.END}")
        print(f"   Total Events: {Colors.GREEN}{stats.total_events}{Colors.END}")
        print(f"   Unique Cows: {Colors.GREEN}{stats.unique_cows}{Colors.END}")
        print(f"   Event Types: {Colors.GREEN}{stats.event_types}{Colors.END}")
        print(f"   First Event: {Colors.YELLOW}{stats.first_event}{Colors.END}")
        print(f"   Last Event: {Colors.YELLOW}{stats.last_event}{Colors.END}")
        
        # Get projection statistics
        result = conn.execute(text("""
            SELECT COUNT(*) as projection_count
            FROM operational.cows
        """))
        proj_stats = result.fetchone()
        
        print(f"\n{Colors.CYAN}📈 Read Projections:{Colors.END}")
        print(f"   Projected Cows: {Colors.GREEN if proj_stats.projection_count > 0 else Colors.RED}{proj_stats.projection_count}{Colors.END}")
        
        if proj_stats.projection_count == 0:
            print(f"   {Colors.YELLOW}⚠️  Projections not synced yet{Colors.END}")
        
        # Show data flow visualization
        print(f"\n{Colors.BOLD}{Colors.HEADER}Data Flow Architecture:{Colors.END}\n")
        
        events_status = "✅ POPULATED" if stats.total_events > 0 else "❌ EMPTY"
        proj_status = "✅ SYNCED" if proj_stats.projection_count > 0 else "⚠️  WAITING"
        bronze_status = "⏳ READY"
        silver_status = "⏳ READY"
        gold_status = "⏳ READY"
        
        print(f"""
    {Colors.BOLD}┌──────────────┐{Colors.END}      {Colors.BOLD}┌──────────────┐{Colors.END}      {Colors.BOLD}┌──────────────┐{Colors.END}
    │   {Colors.CYAN}API Write{Colors.END}  │─────▶│  {Colors.GREEN}Event Store{Colors.END} │─────▶│   {Colors.BLUE}Bronze Δ{Colors.END}   │
    │  {Colors.CYAN}(FastAPI){Colors.END}   │      │  {Colors.GREEN}(SQL){Colors.END}       │      │  {Colors.BLUE}(Delta){Colors.END}    │
    └──────────────┘      └──────────────┘      └──────────────┘
         {Colors.GREEN}✅ ACTIVE{Colors.END}         {events_status}           {bronze_status}
                                 │
                                 │ sync
                                 ▼
                         ┌──────────────┐      ┌──────────────┐
                         │  {Colors.YELLOW}Projection{Colors.END}  │◀─────│   {Colors.CYAN}Silver{Colors.END}     │
                         │  {Colors.YELLOW}(CQRS){Colors.END}      │      │  {Colors.CYAN}(SCD2){Colors.END}     │
                         └──────────────┘      └──────────────┘
                            {proj_status}           {silver_status}
                                                       │
                                                       ▼
                                                ┌──────────────┐
                                                │   {Colors.YELLOW}Gold{Colors.END}      │
                                                │  {Colors.YELLOW}(Analytics){Colors.END} │
                                                └──────────────┘
                                                   {gold_status}
        """)
        
        # Show recent events
        print(f"\n{Colors.BOLD}{Colors.CYAN}Recent Events:{Colors.END}\n")
        
        result = conn.execute(text("""
            SELECT TOP 10
                e.event_time,
                e.event_type,
                e.cow_id,
                e.payload
            FROM operational.cow_events e
            ORDER BY e.event_time DESC
        """))
        
        for event in result:
            import json
            payload = json.loads(event.payload)
            cow_name = payload.get('name', 'Unknown')
            cow_tag = payload.get('tag_number', 'N/A')
            
            event_color = Colors.GREEN if 'created' in event.event_type else Colors.YELLOW
            print(f"   {Colors.BLUE}{event.event_time}{Colors.END} │ {event_color}{event.event_type:20s}{Colors.END} │ {cow_name} ({cow_tag})")
        
        # Show Bronze layer status
        import os
        bronze_path = "/home/xuxoramos/endymion-ai/spark-warehouse/bronze"
        
        print(f"\n{Colors.BOLD}{Colors.BLUE}Bronze Layer Status:{Colors.END}\n")
        
        if os.path.exists(bronze_path):
            files = os.listdir(bronze_path)
            if files:
                print(f"   {Colors.GREEN}✅ Bronze Delta tables exist{Colors.END}")
                print(f"   Location: {bronze_path}")
                print(f"   Files: {len(files)}")
            else:
                print(f"   {Colors.YELLOW}⚠️  Bronze directory exists but empty{Colors.END}")
        else:
            print(f"   {Colors.YELLOW}⚠️  Bronze layer not yet initialized{Colors.END}")
            print(f"   Run: python databricks/bronze/setup_bronze.py")
        
        # Summary
        print(f"\n{Colors.BOLD}{Colors.HEADER}Summary:{Colors.END}\n")
        
        print(f"   {Colors.GREEN}✅{Colors.END} Event sourcing: {stats.total_events} events captured")
        
        if proj_stats.projection_count > 0:
            print(f"   {Colors.GREEN}✅{Colors.END} CQRS projections: {proj_stats.projection_count} records synced")
        else:
            print(f"   {Colors.YELLOW}⚠️{Colors.END}  CQRS projections: Waiting for sync job")
            print(f"      {Colors.CYAN}→ Start: python -m backend.jobs.sync_scheduler{Colors.END}")
        
        print(f"   {Colors.BLUE}ℹ️{Colors.END}  Bronze layer: Ready for ingestion")
        print(f"      {Colors.CYAN}→ Run: python databricks/bronze/ingest_from_sql.py --once{Colors.END}")
        
        print(f"   {Colors.BLUE}ℹ️{Colors.END}  Silver layer: Ready for processing")
        print(f"   {Colors.BLUE}ℹ️{Colors.END}  Gold layer: Ready for analytics")
        
        print()

if __name__ == '__main__':
    main()
