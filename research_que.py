from dagster import op, DagsterType, List, job , DependencyDefinition,  In, Out , graph
from sqlalchemy import create_engine, text
import pandas as pd
import plotly.express as px
 
@op
def visualize_cases():
   
 
    # Setup for connecting to the PostgreSQL database
    database_url = "postgresql://postgres:dapdb7@localhost:5432/dapdata"
    data_engine = create_engine(database_url)
 
    # A SQL query that retrieves the number of cases by their types for specified Area Planning Commissions
    query = text("""
    SELECT "Area_Planning_Commission_APC" as planning_area, "Case_Type" as case_type, COUNT(*) as number_of_cases
    FROM datatable
    WHERE "Area_Planning_Commission_APC" IN ('South Los Angeles', 'North Valley', 'South Valley', 'Central', 'East Los Angeles')
    GROUP BY "Area_Planning_Commission_APC", "Case_Type"
    ORDER BY "Area_Planning_Commission_APC", number_of_cases DESC
    """)
 
    # Executing the SQL query and converting the results into a pandas DataFrame
    with data_engine.connect() as connection:
        query_result = connection.execute(query)
        case_data = pd.DataFrame(query_result.fetchall(), columns=query_result.keys())
 
    # Isolate the top 5 case types within each Area Planning Commission for clarity in visualization
    top_cases_by_area = case_data.groupby('planning_area').apply(lambda x: x.nlargest(5, 'number_of_cases')).reset_index(drop=True)
 
    # Create a bar chart showing the distribution of case types across different Area Planning Commissions
    chart = px.bar(top_cases_by_area, x='planning_area', y='number_of_cases', color='case_type',
                   title='Top 5 Case Types by Area Planning Commission', labels={'number_of_cases': 'Number of Cases'},
                   category_orders={"planning_area": ["South Los Angeles", "North Valley", "South Valley", "Central", "East Los Angeles"]})  # Ensures a specific display order
    chart.update_layout(xaxis_title="Area Planning Commission", yaxis_title="Number of Cases", barmode='group')
    chart.show()
 
@op
def analyze_enforcement_cases():
    # Set up a connection to the PostgreSQL database
    database_engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
    database_connection = database_engine.connect()
 
    # SQL query to select cases with valid dates and numbers
    data_query = text("""
    SELECT "Date_Case_Generated", "Case_Number"
    FROM datatable
    WHERE "Date_Case_Generated" IS NOT NULL AND "Case_Number" IS NOT NULL;
    """)
 
    # Execute the query and fetch the results
    query_results = database_connection.execute(data_query)
 
    # Convert query results to a pandas DataFrame
    case_data = pd.DataFrame(query_results.fetchall(), columns=query_results.keys())
 
    # Close the database connection
    database_connection.close()
    database_engine.dispose()
 
    # Convert the date field to the datetime type to enable time series analysis
    case_data["Date_Case_Generated"] = pd.to_datetime(case_data["Date_Case_Generated"], format='%Y-%m-%dT%H:%M:%S')
 
    # Extract the month and year from the date for grouping
    case_data["Month"] = case_data["Date_Case_Generated"].dt.month
    case_data["Year"] = case_data["Date_Case_Generated"].dt.year
 
    # Aggregate the data by month and year to count the cases
    cases_by_month_year = case_data.groupby(["Year", "Month"]).size().reset_index(name="Number of Cases")
 
    # Create a line chart to display the number of cases per month for each year
    seasonal_trend_chart = px.line(cases_by_month_year, x="Month", y="Number of Cases", color="Year",
                                   labels={"Month": "Month", "Number of Cases": "Number of Cases"},
                                   title="Seasonal Patterns of Code Enforcement Cases")
    seasonal_trend_chart.update_xaxes(tickmode='array', tickvals=list(range(1, 13)), ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    seasonal_trend_chart.update_layout(xaxis_title="Month", yaxis_title="Number of Cases", legend_title="Year")
    seasonal_trend_chart.show()
 
@op
def visualize_permit_counts():
    # Establish a connection to the PostgreSQL database
    database_engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
    database_connection = database_engine.connect()
 
    # SQL query to fetch the count of different types of permits issued each year
    permit_count_query = text("""
    SELECT EXTRACT(YEAR FROM "CofO Issue Date")::INT AS "Year", "Permit Type", COUNT(*) AS "Count"
    FROM datatable
    WHERE "CofO Issue Date" IS NOT NULL AND "Permit Type" IS NOT NULL
    GROUP BY EXTRACT(YEAR FROM "CofO Issue Date"), "Permit Type"
    ORDER BY "Year";
    """)
 
    # Execute the query to retrieve permit count data
    permit_data_result = database_connection.execute(permit_count_query)
 
    # Load the query results into a pandas DataFrame for easier manipulation and visualization
    permit_counts_dataframe = pd.DataFrame(permit_data_result.fetchall(), columns=["Year", "Permit Type", "Count"])
 
    # Safely close the connection and dispose of the database engine
    database_connection.close()
    database_engine.dispose()
 
    # Generate a box plot to visualize how permit counts vary over time by type
    permit_distribution_chart = px.box(permit_counts_dataframe, x="Permit Type", y="Count", color="Permit Type",
                                       labels={"Count": "Number of Permits", "Permit Type": "Type of Permit"},
                                       title="Distribution of Permit Counts by Type Over Years")
    permit_distribution_chart.update_layout(
        xaxis_title="Type of Permit",
        yaxis_title="Number of Permits",
        legend_title="Permit Type",
        margin=dict(l=20, r=20, t=30, b=100)  # More space for labels
    )
    permit_distribution_chart.show()
 
@op
def analyze_high_valuation_projects():
    # Set up a connection to the PostgreSQL database
    database_engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
    database_connection = database_engine.connect()
 
    # SQL query to fetch project records with valuations greater than $1 million
    million_dollar_project_query = text("SELECT * FROM datatable WHERE \"Valuation\" > 1000000;")
 
    # Execute the query and fetch results
    million_dollar_results = database_connection.execute(million_dollar_project_query)
 
    # Convert results to a pandas DataFrame for analysis
    projects_dataframe = pd.DataFrame(million_dollar_results.fetchall(), columns=million_dollar_results.keys())
 
    # Clean up by closing the connection and disposing of the engine
    database_connection.close()
    database_engine.dispose()
 
    # Identify and visualize numerical data fields for high-valuation projects
    numerical_data_columns = projects_dataframe.select_dtypes(include=['int64', 'float64']).columns
 
    # Create a single figure for all histograms
    combined_histogram_figure = px.histogram(projects_dataframe,
                                            x=numerical_data_columns,
                                            nbins=20,
                                            marginal="rug",
                                            title="Distribution of Numerical Columns for Projects Exceeding $1 Million")
    combined_histogram_figure.update_layout(yaxis_title="Frequency", template="plotly_white")
    combined_histogram_figure.show()  
 
@op
def visualize_enforcement_case_trends():
    # Establish connection to PostgreSQL database
    database_engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
    database_connection = database_engine.connect()
 
    # SQL query to select cases with valid dates and types
    case_data_query = text("""
    SELECT "Date_Case_Generated", "Case_Type"
    FROM datatable
    WHERE "Date_Case_Generated" IS NOT NULL AND "Case_Type" IS NOT NULL;
    """)
 
    # Execute the query and retrieve results
    case_query_result = database_connection.execute(case_data_query)
 
    # Load the data into a pandas DataFrame for analysis
    case_data_frame = pd.DataFrame(case_query_result.fetchall(), columns=case_query_result.keys())
 
    # Safely close the database connection
    database_connection.close()
    database_engine.dispose()
 
    # Process date information for analysis
    case_data_frame['Date_Case_Generated'] = pd.to_datetime(case_data_frame['Date_Case_Generated'])
    case_data_frame['Year'] = case_data_frame['Date_Case_Generated'].dt.year
    case_data_frame['Month'] = case_data_frame['Date_Case_Generated'].dt.month
 
    # Filter data for the last 10 years
    latest_year = case_data_frame['Year'].max()
    recent_case_data = case_data_frame[case_data_frame['Year'] >= latest_year - 9]
 
    # Aggregate data by year, month, and case type
    case_summary = recent_case_data.groupby(['Year', 'Month', 'Case_Type']).size().reset_index(name='Case Counts')
 
    # Create a pivot table for visualization
    case_pivot = case_summary.pivot_table(index=['Year', 'Month'], columns='Case_Type', values='Case Counts', fill_value=0).reset_index()
 
    # Prepare data for plotting
    plot_data = case_pivot.melt(id_vars=['Year', 'Month'], var_name='Case_Type', value_name='Case Counts')
 
    # Generate an area chart to show trends over time
    trend_chart = px.area(plot_data, x='Month', y='Case Counts', color='Case_Type', line_group='Year',
                          labels={'Month': 'Month of the Year', 'Case Counts': 'Number of Cases'},
                          title='Monthly Variation of Enforcement Case Types Over the Last 10 Years')
    trend_chart.update_xaxes(dtick=1, tickmode='array', tickvals=list(range(1, 13)),
                             ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    trend_chart.update_layout(xaxis_title="Month", yaxis_title="Number of Cases", legend_title="Case Type")
    trend_chart.show()
 
@op
def visualize_zoning_trends():
    # Set up a connection to the PostgreSQL database
    database_engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
    database_connection = database_engine.connect()
 
    # SQL query to fetch zoning data where issue dates and zones are not null
    zoning_query = text("""
    SELECT "CofO Issue Date", "ZONE"
    FROM datatable
    WHERE "CofO Issue Date" IS NOT NULL AND "ZONE" IS NOT NULL;
    """)
 
    # Execute the query and retrieve zoning data
    zoning_data_result = database_connection.execute(zoning_query)
 
    # Convert the query results into a pandas DataFrame for further analysis
    zoning_dataframe = pd.DataFrame(zoning_data_result.fetchall(), columns=zoning_data_result.keys())
 
    # Close the database connection to release resources
    database_connection.close()
    database_engine.dispose()
 
    # Process date information to prepare for analysis
    zoning_dataframe['CofO Issue Date'] = pd.to_datetime(zoning_dataframe['CofO Issue Date'])
    zoning_dataframe['Year'] = zoning_dataframe['CofO Issue Date'].dt.year
 
    # Filter the DataFrame to only include data from the last 10 years
    current_year = zoning_dataframe['Year'].max()
    recent_zoning_data = zoning_dataframe[zoning_dataframe['Year'] >= current_year - 9]
 
    # Identify the top 5 most common zoning types
    top_zoning_types = recent_zoning_data['ZONE'].value_counts().nlargest(5).index
 
    # Narrow down the data to only include these top zoning types
    top_zones_data = recent_zoning_data[recent_zoning_data['ZONE'].isin(top_zoning_types)]
 
    # Group data by year and zone to count occurrences
    zone_counts_by_year = top_zones_data.groupby(['Year', 'ZONE']).size().reset_index(name='Building Counts')
 
    # Sort data for better visualization purposes
    sorted_zone_counts = zone_counts_by_year.sort_values(by=['Year', 'Building Counts'], ascending=[True, False])
 
    # Create a bar chart to visualize the number of newly certified buildings by zoning type over the years
    zoning_trends_chart = px.bar(sorted_zone_counts, x='Year', y='Building Counts', color='ZONE', barmode='group',
                                 title='Top 5 Zoning Types for Newly Certified Buildings Over the Last 10 Years',
                                 labels={'Building Counts': 'Number of Buildings', 'Year': 'Year'})
    zoning_trends_chart.update_layout(xaxis_title="Year", yaxis_title="Number of Newly Certified Buildings", legend_title="Zoning Type")
    zoning_trends_chart.show()
 
@op
def visualize_occupancy_certificate_trends():
    # Establish a connection to the PostgreSQL database
    database_engine = create_engine('postgresql://postgres:dapdb7@localhost:5432/dapdata')
    database_connection = database_engine.connect()
 
    # SQL query to fetch occupancy data with valid issue dates and census tracts
    occupancy_data_query = text("""
    SELECT "CofO Issue Date", "Census Tract"
    FROM datatable
    WHERE "CofO Issue Date" IS NOT NULL AND "Census Tract" IS NOT NULL;
    """)
 
    # Execute the query and retrieve the data
    occupancy_query_results = database_connection.execute(occupancy_data_query)
 
    # Convert the query results into a pandas DataFrame for analysis
    occupancy_dataframe = pd.DataFrame(occupancy_query_results.fetchall(), columns=occupancy_query_results.keys())
 
    # Close the database connection after fetching the data
    database_connection.close()
    database_engine.dispose()
 
    # Process date information to prepare for analysis
    occupancy_dataframe['CofO Issue Date'] = pd.to_datetime(occupancy_dataframe['CofO Issue Date'])
    occupancy_dataframe['Year'] = occupancy_dataframe['CofO Issue Date'].dt.year
 
    # Filter the DataFrame to only include data from the last 10 years
    latest_year = occupancy_dataframe['Year'].max()
    recent_occupancy_data = occupancy_dataframe[occupancy_dataframe['Year'] >= latest_year - 9]
 
    # Identify the top 10 census tracts with the most occurrences
    frequent_census_tracts = recent_occupancy_data['Census Tract'].value_counts().nlargest(10).index
 
    # Narrow down the data to only include these top census tracts
    top_tracts_data = recent_occupancy_data[recent_occupancy_data['Census Tract'].isin(frequent_census_tracts)]
 
    # Aggregate data by year and census tract to count the occurrences
    census_tract_trends = top_tracts_data.groupby(['Year', 'Census Tract']).size().reset_index(name='Certificate Count')
 
    # Sort the data for better visualization
    sorted_census_tract_trends = census_tract_trends.sort_values(by=['Year', 'Certificate Count'], ascending=[True, False])
 
    # Generate a line chart to visualize the trends of occupancy certificates issuance
    occupancy_trend_chart = px.line(sorted_census_tract_trends, x='Year', y='Certificate Count', color='Census Tract',
                                    title='Number of Occupancy Certificates Issued Over the Last 10 Years by Top 10 Census Tracts',
                                    labels={'Certificate Count': 'Number of Occupancy Certificates', 'Year': 'Year'})
    occupancy_trend_chart.update_layout(xaxis_title="Year", yaxis_title="Number of Occupancy Certificates", legend_title="Census Tract")
    occupancy_trend_chart.show()
 
 
 
 
 
 
@job
def dap_visuals_graph():
    visualize_cases()
    analyze_enforcement_cases()
    visualize_permit_counts()
    analyze_high_valuation_projects()
    visualize_enforcement_case_trends()
    visualize_zoning_trends()
    visualize_occupancy_certificate_trends()
 