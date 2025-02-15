import streamlit as st
import pandas as pd
from court_data import get_db_connection
import plotly.graph_objects as go

def get_court_types_hierarchy():
    """Get court types with their hierarchy"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        WITH RECURSIVE court_hierarchy AS (
            -- Base case: top-level courts (no parent)
            SELECT 
                id, name, level, description, parent_type_id,
                ARRAY[name]::varchar[] as path,
                1 as depth
            FROM court_types 
            WHERE parent_type_id IS NULL

            UNION ALL

            -- Recursive case: courts with parents
            SELECT 
                ct.id, ct.name, ct.level, ct.description, ct.parent_type_id,
                ch.path || ct.name::varchar,
                ch.depth + 1
            FROM court_types ct
            JOIN court_hierarchy ch ON ct.parent_type_id = ch.id
        )
        SELECT * FROM court_hierarchy
        ORDER BY path;
    """)

    hierarchy = cur.fetchall()
    cur.close()
    conn.close()
    return hierarchy

def get_jurisdictions():
    """Get all jurisdictions with their types and court counts"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            j1.name, 
            j1.type, 
            j2.name as parent_jurisdiction,
            COUNT(c.id) as court_count
        FROM jurisdictions j1
        LEFT JOIN jurisdictions j2 ON j1.parent_id = j2.id
        LEFT JOIN courts c ON c.jurisdiction_id = j1.id
        GROUP BY j1.id, j1.name, j1.type, j2.name
        ORDER BY j1.type, j1.name;
    """)

    jurisdictions = cur.fetchall()
    cur.close()
    conn.close()
    return jurisdictions

# Page configuration
st.set_page_config(
    page_title="Court Hierarchy | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court System Hierarchy")
st.markdown("Explore the complete structure of the U.S. court system across federal, state, and county levels")

# Create tabs for different views
system_tab, jurisdictions_tab = st.tabs(["Court System", "Jurisdictions"])

with system_tab:
    hierarchy = get_court_types_hierarchy()

    # Group courts by level
    courts_by_level = {}
    for court in hierarchy:
        level = court[2]  # Level is at index 2
        if level not in courts_by_level:
            courts_by_level[level] = []
        courts_by_level[level].append(court)

    # Display courts by level with expandable sections
    for level in sorted(courts_by_level.keys()):
        with st.expander(f"Level {level} Courts", expanded=True):
            for court in courts_by_level[level]:
                indent = "  " * (court[6] - 1)  # Use depth for indentation
                st.markdown(f"""
                {indent}• **{court[1]}**  
                {indent}  {court[3]}
                """)

with jurisdictions_tab:
    jurisdictions = get_jurisdictions()

    # Filter controls
    col1, col2 = st.columns([2, 1])
    with col1:
        jurisdiction_type = st.selectbox(
            "Filter by Jurisdiction Type",
            options=["All", "Federal", "State", "County"],
            index=0
        )

    # Filter jurisdictions based on selection
    filtered_jurisdictions = [
        j for j in jurisdictions 
        if jurisdiction_type == "All" or j[1].lower() == jurisdiction_type.lower()
    ]

    # Create visualization based on jurisdiction type
    if jurisdiction_type in ["All", "State"]:
        # Show state map for state jurisdictions
        state_jurisdictions = [j for j in filtered_jurisdictions if j[1] == 'state']
        if state_jurisdictions:
            st.subheader("State Jurisdictions Map")
            states = pd.DataFrame([
                {'state': j[0], 'courts': j[3]} 
                for j in state_jurisdictions
            ])

            fig = go.Figure(data=go.Choropleth(
                locations=states['state'],
                locationmode='USA-states',
                z=states['courts'],
                colorscale=[[0, '#f0f2f6'], [1, '#0B3D91']],
                colorbar_title="Number of Courts"
            ))

            fig.update_layout(
                geo_scope='usa',
                margin={"r":0,"t":0,"l":0,"b":0},
                height=300
            )

            st.plotly_chart(fig, use_container_width=True)

    # Display jurisdiction details in a structured format
    st.subheader("Jurisdiction Details")

    for j_type in ['federal', 'state', 'county']:
        if jurisdiction_type in ["All", j_type.title()]:
            type_jurisdictions = [j for j in filtered_jurisdictions if j[1] == j_type]
            if type_jurisdictions:
                with st.expander(f"{j_type.title()} Jurisdictions", expanded=True):
                    for j in type_jurisdictions:
                        parent = f" (Part of {j[2]})" if j[2] else ""
                        courts = f"({j[3]} courts)" if j[3] else "(No courts)"
                        st.markdown(f"• **{j[0]}** {parent} {courts}")

# Add comprehensive explanatory text
st.markdown("""
### Understanding the U.S. Court System

The United States court system is organized into several hierarchical levels:

#### Federal Courts
1. **Supreme Court**
   - Highest court in the federal system
   - Final interpreter of the Constitution
   - Nine Supreme Court Justices

2. **Courts of Appeals (Circuit Courts)**
   - 13 appellate courts
   - Review decisions from district courts
   - Handle appeals from federal administrative agencies

3. **District Courts**
   - Primary trial courts of the federal system
   - At least one in each state
   - Handle federal criminal and civil cases

#### State Courts
1. **State Supreme Courts**
   - Highest court in each state
   - Final interpreter of state law
   - Handle appeals from lower state courts

2. **State Appellate Courts**
   - Intermediate appeals courts
   - Review decisions from trial courts
   - Not all states have them

3. **State Trial Courts**
   - Handle most criminal and civil cases
   - Various divisions (civil, criminal, family)
   - Most cases begin here

#### County Courts
1. **Superior Courts**
   - Main trial courts at county level
   - Handle major civil and criminal cases
   - Some have specialized divisions

2. **Specialized County Courts**
   - Family Courts
   - Probate Courts
   - Juvenile Courts
   - Criminal Courts
   - Civil Courts

#### Other Courts
1. **Municipal Courts**
   - City-level courts
   - Handle local ordinance violations
   - Traffic cases and minor offenses

2. **Tribal Courts**
   - Courts of sovereign tribal nations
   - Handle tribal law matters
   - May have concurrent jurisdiction with state/federal courts

3. **Administrative Courts**
   - Executive branch courts
   - Handle specific agency matters
   - Examples: Tax Court, Immigration Courts
""")