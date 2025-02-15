import plotly.express as px
import plotly.graph_objects as go

def create_court_map(df, selected_court=None):
    fig = go.Figure()

    # Base map
    fig.add_trace(go.Scattergeo(
        locationmode='USA-states',
        lon=df['lon'],
        lat=df['lat'],
        text=df['name'],
        mode='markers',
        marker=dict(
            size=10,
            color='#0B3D91',
            symbol='circle'
        ),
        hovertemplate="<b>%{text}</b><br>" +
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
                marker=dict(
                    size=15,
                    color='#dc3545',
                    symbol='circle'
                ),
                hovertemplate="<b>%{text}</b><br>" +
                            "Currently selected<extra></extra>"
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
        showlegend=False
    )

    return fig
