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
                ARRAY[name] as path,
                1 as depth
            FROM court_types 
            WHERE parent_type_id IS NULL
            
            UNION ALL
            
            -- Recursive case: courts with parents
            SELECT 
                ct.id, ct.name, ct.level, ct.description, ct.parent_type_id,
                ch.path || ct.name,
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
    """Get all jurisdictions with their types"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            j1.name, j1.type, j2.name as parent_jurisdiction
        FROM jurisdictions j1
        LEFT JOIN jurisdictions j2 ON j1.parent_id = j2.id
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
st.markdown("Explore the structure of the U.S. court system")

# Create two columns for the main layout
col1, col2 = st.columns([3, 2])

with col1:
    st.header("Court Types")
    hierarchy = get_court_types_hierarchy()
    
    # Create a tree-like visualization using indentation
    for court in hierarchy:
        indent = "  " * (court[6] - 1)  # Use depth for indentation
        st.markdown(f"""
        {indent}• **{court[1]}**  
        {indent}  *Level {court[2]}* - {court[3]}
        """)

with col2:
    st.header("Jurisdictions")
    jurisdictions = get_jurisdictions()
    
    # Create tabs for different jurisdiction types
    fed_tab, state_tab = st.tabs(["Federal", "State"])
    
    with fed_tab:
        fed_jurisdictions = [j for j in jurisdictions if j[1] == 'federal']
        for j in fed_jurisdictions:
            st.markdown(f"**{j[0]}**")
            
    with state_tab:
        state_jurisdictions = [j for j in jurisdictions if j[1] == 'state']
        # Create a US map visualization
        states = pd.DataFrame([
            {'state': j[0], 'value': 1} 
            for j in state_jurisdictions
        ])
        
        fig = go.Figure(data=go.Choropleth(
            locations=states['state'],
            locationmode='USA-states',
            z=states['value'],
            colorscale=[[0, '#f0f2f6'], [1, '#0B3D91']],
            showscale=False
        ))
        
        fig.update_layout(
            geo_scope='usa',
            margin={"r":0,"t":0,"l":0,"b":0},
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # List all states
        st.markdown("### State Jurisdictions")
        cols = st.columns(3)
        for i, j in enumerate(sorted(state_jurisdictions, key=lambda x: x[0])):
            cols[i % 3].markdown(f"• {j[0]}")

# Add explanatory text
st.markdown("""
### About the Court System
The United States court system is organized into several levels and jurisdictions:

1. **Federal Courts**
   - Supreme Court: The highest court in the federal system
   - Courts of Appeals: Circuit courts that hear appeals from district courts
   - District Courts: Federal trial courts
   - Specialized Courts: Including bankruptcy courts and others

2. **State Courts**
   - State Supreme Courts: Highest courts in state systems
   - State Appellate Courts: Intermediate appeals courts
   - State Trial Courts: Primary trial courts
   - Specialized State Courts: Courts for specific types of cases

3. **Other Courts**
   - Tribal Courts: Courts of sovereign tribal nations
   - Administrative Courts: Executive branch courts
""")
