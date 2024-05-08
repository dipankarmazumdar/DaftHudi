import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
import daft
import os
from dotenv import load_dotenv

load_dotenv()

@st.cache_data(ttl=3600, show_spinner=False)
def load_data():
    session = boto3.session.Session()
    creds = session.get_credentials()
    io_config = daft.io.IOConfig(
        s3=daft.io.S3Config(
            access_key=creds.secret_key,
            key_id=creds.access_key,
            session_token=creds.token,
            region_name="us-west-2",
        )
    )
    df = daft.read_hudi("s3://my-bucket/sandbox/daft_hudi", io_config=io_config)
    df_analysis = df.select("supermarket", "prices", "names", "date", "own_brand", "category")
    df_analysis.collect()
    df_analysis.explain(show_all=True)

    # Convert 'prices' to numeric immediately after loading
    df_analysis = df_analysis.with_column(
        "prices", df_analysis["prices"].cast(daft.DataType.float64())
    )

    distinct_names_per_category = df_analysis.select("category", "names").distinct()
    category_diversity_daft = distinct_names_per_category.groupby("category").agg(
        daft.col('names').count()
    ).to_pandas()
    category_diversity_daft.columns = ['Category', 'Number of Unique Products']

    # Convert full Daft DataFrame to Pandas for caching and dynamic filtering
    df_full = df_analysis.to_pandas()
    df_full['own_brand'] = df_full['own_brand'].replace({'FALSE': False, 'True': True}).astype(bool)

    return df_full, category_diversity_daft

df_full, category_diversity_pandas = load_data()

st.set_page_config(page_title="Hudi Streamlit Application", layout="wide")
st.title('Apache Hudi - Daft Dataframe')

selected_categories = st.multiselect('Select Categories', options=['All'] + sorted(df_full['category'].unique()), default=['All'])

# Conditionally apply filter
if 'All' in selected_categories or not selected_categories:
    df_filtered = df_full
else:
    df_filtered = df_full[df_full['category'].isin(selected_categories)]

modern_mint = ['#1abc9c', '#16a085', '#2ecc71', '#27ae60', '#3498db']

col1, col2 = st.columns(2, gap="large")
with col1:
    st.subheader('Price Distribution by Category')
    fig1 = px.box(df_filtered, x='category', y='prices', title='Price Distribution by Category', color_discrete_sequence=modern_mint)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader('Product Variety per Category')
    filtered_category_diversity = category_diversity_pandas[category_diversity_pandas['Category'].isin(df_filtered['category'].unique())]
    fig_category_diversity = px.bar(filtered_category_diversity, x='Category', y='Number of Unique Products', title='Product Variety per Category', color_discrete_sequence=modern_mint)
    st.plotly_chart(fig_category_diversity, use_container_width=True)

col3, col4 = st.columns(2, gap="large")
with col3:
    st.subheader('Share of Own Brand Products')
    own_brand_count = df_filtered['own_brand'].value_counts(normalize=True).reset_index(name='proportion')
    fig4 = px.pie(own_brand_count, values='proportion', names='own_brand', title='Share of Own Brand Products', labels={'own_brand': 'Brand Type'}, color_discrete_sequence=modern_mint)
    st.plotly_chart(fig4, use_container_width=True)

with col4:
    st.subheader('Average Price by Brand Type and Category')
    # Group by both 'own_brand' and 'category' for a more detailed breakdown
    brand_category_price_comparison = df_filtered.groupby(['own_brand', 'category'])['prices'].mean().unstack().fillna(0)
    fig5 = px.bar(brand_category_price_comparison, title='Average Price by Brand Type and Category', color_discrete_sequence=modern_mint, labels={'value':'Average Price', 'variable':'Category'})
    fig5.update_layout(barmode='stack')
    st.plotly_chart(fig5, use_container_width=True)

