import streamlit as st
from datetime import datetime

def display_court_info(court_data):
    if court_data is None:
        return

    # Create two columns
    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(f"## {court_data['name']}")
        st.markdown(f"**Type:** {court_data['type']}")

        # Status with colored indicator
        status_color = {
            'Open': 'status-open',
            'Closed': 'status-closed',
            'Limited Operations': 'status-limited'
        }
        st.markdown(f"**Status:** <span class='{status_color[court_data['status']]}'>{court_data['status']}</span>", 
                   unsafe_allow_html=True)

        st.markdown(f"**Address:** {court_data['address']}")

        # Display maintenance information if available
        if court_data.get('maintenance_notice'):
            st.markdown("### Maintenance Notice")
            st.warning(court_data['maintenance_notice'])

            # Display maintenance dates if available
            if court_data.get('maintenance_start') or court_data.get('maintenance_end'):
                date_info = []
                if court_data.get('maintenance_start'):
                    start_date = court_data['maintenance_start']
                    if isinstance(start_date, str):
                        start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%B %d, %Y')
                    date_info.append(f"Start: {start_date}")
                if court_data.get('maintenance_end'):
                    end_date = court_data['maintenance_end']
                    if isinstance(end_date, str):
                        end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%B %d, %Y')
                    date_info.append(f"End: {end_date}")

                st.markdown("**Maintenance Period:**")
                st.markdown(" • " + "\n • ".join(date_info))

    with col2:
        st.image(court_data['image_url'], 
                caption="Court Building",
                use_column_width=True)

def display_status_legend():
    st.markdown("### Status Legend")
    st.markdown("""
    <div>
        <span class='status-open'>● Open</span><br>
        <span class='status-closed'>● Closed</span><br>
        <span class='status-limited'>● Limited Operations</span>
    </div>
    """, unsafe_allow_html=True)