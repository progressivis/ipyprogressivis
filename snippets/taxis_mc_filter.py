# progressivis-snippets
@register_function
def mc_filter(df):
    pklon = df['pickup_longitude']
    pklat = df['pickup_latitude']
    dolon = df['dropoff_longitude']
    dolat = df['dropoff_latitude']
    return df[(pklon > -74.08) & (pklon < -73.5) &
              (pklat > 40.55) & (pklat < 41.00) &
              (dolon > -74.08) & (dolon < -73.5) &
              (dolat > 40.55) & (dolat < 41.00)]
