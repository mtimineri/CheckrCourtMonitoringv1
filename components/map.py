import plotly.express as px
import plotly.graph_objects as go

def create_court_map(df, selected_court=None):
    # Define status colors
    status_colors = {
        'Open': '#28a745',  # Green
        'Closed': '#dc3545',  # Red
        'Limited Operations': '#ffc107'  # Yellow
    }

    fig = go.Figure()

    # Create a trace for each status
    for status in df['status'].unique():
        status_df = df[df['status'] == status]

        fig.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=status_df['lon'],
            lat=status_df['lat'],
            text=status_df['name'],
            mode='markers',
            name=status,  # This will create a legend entry
            marker=dict(
                size=10,
                color=status_colors.get(status, '#0B3D91'),  # Use default blue if status not found
                symbol='circle'
            ),
            hovertemplate="<b>%{text}</b><br>" +
                         "Status: " + status + "<br>" +
                         "Click for more information<extra></extra>"
        ))

    # Highlight selected court if any
    if selected_court is not None:
        selected_df = df[df['name'] == selected_court]
        if not selected_df.empty:
            fig.add_trace(go.Scattergeo(
                locationmode='USA-states',
                lon=selected_df['lon'],
                lat=selected_df['lat'],
                text=selected_df['name'],
                mode='markers',
                name='Selected',
                marker=dict(
                    size=15,
                    color='#dc3545',  # Red
                    symbol='circle',
                    line=dict(width=2, color='white')  # Add white border
                ),
                hovertemplate="<b>%{text}</b><br>" +
                            "Currently selected<extra></extra>",
                showlegend=False
            ))

    # Update layout
    fig.update_layout(
        geo=dict(
            scope='usa',
            projection_type='albers usa',
            showland=True,
            landcolor='rgb(243, 243, 243)',
            countrycolor='rgb(204, 204, 204)',
            showlakes=True,
            lakecolor='rgb(255, 255, 255)'
        ),
        margin={"r":0,"t":0,"l":0,"b":0},
        height=600,
        showlegend=True,
        legend_title="Court Status"
    )

    return fig