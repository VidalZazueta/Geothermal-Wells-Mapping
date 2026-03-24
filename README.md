# Geothermal-Wells-Mapping
## Visualizing Geothermal Wells Across California

## Motivation
The motivation for this project is to better understand the role of geothermal energy in California's renewable energy landscape. Geothermal wells are a critical component of sustainable energy production, yet their distribution, operational status, and types are not always clearly understood. By analyzing this dataset, we aim to uncover patterns that can provide insights into how geothermal resources are currently utilized across the state.
The dataset provided by the California Department of Conservation contains information about the world, including the geographic location, status, and type. This data is valuable for energy analysts, policy makers, and environmental researchers who are interested in improving renewable energy strategies. Previous research has primarily focused on energy output, but less attention has been given to special distribution and operational trends of wells.
Our project will use visualization techniques to map my geothermal wells across California and highlight key patterns, such as clusters of activity, inactive wells, and variation in well types. By doing so, we aim to provide an intuitive and accessible way to explore the dataset, enabling users to gain insights that support decision-making in energy planning and environmental management.

## Approach
Our approach begins with a thorough exploration and preprocessing of the geothermal wells dataset. We will first analyze the dataset structure to identify relevant attributes such as geographic coordinates (latitude and longitude), well status, well type, and operator information. Any missing or inconsistent data will be cleaned using Python, ensuring that the dataset is suitable for accurate visualization and analysis.
Once the data is prepared, we will design and implement a series of visualizations using Tableau. A primary component of our system can be a geospatial map that displays the locations of geothermal wells across California. This map will allow users to zoom into specific regions and filter wells by attributes such as status or type. In addition to the map, we’d create bar charts and pie charts to compare the distribution of well statistics and types as well as highlight the most active regions or operators. 

To enhance user interaction, we can incorporate filters and dashboard elements that allow users to dynamically explore the dataset. For example, users will be able to select specific regions or categories to observe how geothermal activity varies across California. The overall goal of our approach is to create a clear, interactive, and insightful visualization system that enables users to easily identify patterns, trends, and anomalies within the dataset. 

## Milestones
We will begin by thoroughly examining the Geothermal Wells dataset to identify which columns are most relevant to our analysis, such as well status, location, depth, operator, and drilling date. Any columns with frequent errors or missing values will be cleaned and corrected before any visualization work begins. Once the data is in a reliable state, we will develop a preliminary version of our dashboards featuring a basic map of well locations and simple bar charts broken down by county and well status. From there, we will iteratively refine the visualizations by incorporating interactivity such as filters and new viewpoints across charts. 
The final version of our dashboard will include the complete set of planned visualizations and polished formatting. Progress will be divided evenly among group members, with regular check-ins to ensure we are on track to meet the presentation deadline.

## Extensions
If the project continued until after our assignment, we could add predictive elements to it. Using machine learning, we could find which areas or wells are likely to go inactive in the near future. If we added another layer to it, we could also map local power lines and areas that could even use geothermal wells. Or wells that, if possible, could be reactivated in the future.

## Tools
Tableau: For building interactive dashboards and geospatial maps.
Python: For data cleaning, handling null values, and formatting the CSV for Tableau.
Excel: For initial data inspection,
GitHub: For version control and sharing our Python scripts among the team.

## References
Dataset: https://lab.data.ca.gov/dataset/calgem-geothermal-wells/651ae213-b4b9-4721-907d-f967e1b7a89e

## Installation
1. Go into desired folder or workspace and clone the repository

2. Create a virtual environment with Python 3.10 or higher
    ### Windows
    - python -m venv .venv

    ### Mac/Linux
    - python3 -m venv .venv

3. Activate the virtual environment
    ### Mac/Linux
    - source .venv/bin/activate

    ### Windows (Command Prompt)
    - .venv\Scripts\activate.bat

    ### Windows (PowerShell)
    - .venv\Scripts\Activate.ps1

Note: If you are using VS Code, it should automatically activate your environment
4. Select the virtual environment in your IDE (e.g., VS Code)
    - Press Ctrl+Shift+P, then select "Python: Select Interpreter"
    - Select "Enter interpreter path..." then select "Find..."
    - When File explorer opens go to env -> Scripts -> select "python.exe"

4. Next run: pip install -r requirements.txt