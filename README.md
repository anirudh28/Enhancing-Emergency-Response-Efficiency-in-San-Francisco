# Enhancing Emergency Response Efficiency in San Francisco

**Anirudh Garg**, Courant Institute of Mathematical Sciences, New York University, New York, USA  
**Haardik Dharma**, Courant Institute of Mathematical Sciences, New York University, New York, USA  
**Rhea Chandok**, Courant Institute of Mathematical Sciences, New York University, New York, USA  

## Abstract
In this paper, we analyze and gain actionable insights into the efficiency of emergency response times and patterns related to fire incidents and emergency medical services (EMS) in San Francisco. Utilizing publicly available datasets on dispatched calls, fire incidents, and EMS response times, we conduct a comprehensive analysis to assess response efficiency, identify common emergency types, and uncover temporal patterns in incident frequency. The findings from this study provide valuable insights to assist public safety departments and policymakers in enhancing preparedness, optimizing resource allocation for emergency services in San Francisco, and helping residents avoid high-incident areas.

**Keywords:** analytics, fire, calls, ems, response times, San Francisco

---

## I. INTRODUCTION
Emergency response services play a critical role in ensuring public safety and minimizing the impact of incidents in urban environments. In San Francisco, a city known for its diverse geography and dense population, the efficiency of fire and emergency medical services (EMS) is paramount. This project focuses on analyzing the performance and patterns of these essential services to provide data-driven insights for improvement.

In our analysis, we delve into various aspects of emergency response and incident patterns in San Francisco, leveraging the rich datasets available. Our comprehensive approach examines both fire and medical emergency services, providing a multifaceted view of the city's emergency response landscape. The study encompasses several key areas of investigation, including response time efficiency across different regions for both medical and fire emergencies, the geographical distribution of fire incidents, and the prevalence of various emergency types.

We also explore temporal trends, analyzing yearly and monthly patterns in emergency occurrences to identify potential seasonal influences or long-term shifts. Additionally, our research examines the distribution of call types and their regional variations, offering insights into the diverse nature of emergencies across San Francisco's neighborhoods.

To assess the human impact of fire incidents, we investigate fatalities and injuries concerning specific zip codes. Furthermore, we conduct a comparative analysis of turnaround times for medical and fire responses, including exploring factors influencing fire response durations.

---

## II. MOTIVATION
Emergency response services are the lifeline of urban safety, yet their effectiveness can be significantly enhanced through data-driven insights. Emergency incidents rarely occur randomly. The vast archives of historical emergency data contain valuable patterns and trends that can revolutionize how we approach public safety.

By leveraging these extensive datasets, we can uncover hidden factors influencing emergency response times, incident frequencies, and outcomes that may not be immediately apparent to even the most experienced first responders.

---

## III. RELATED WORK
The analysis of emergency response efficiency has been a growing area of interest due to its potential to improve public safety outcomes and optimize resource allocation. Several studies have explored various aspects of emergency response using data analytics techniques. This section highlights some of the key research relevant to our project.

- **Predictive modeling for EMS**: Research on predictive models for EMS using machine learning, such as Random Forest and decision trees, has gained attention due to its potential in forecasting EMS demand patterns.
- **Ambulance allocation optimization**: Acuna et al. proposed a model for optimizing ambulance allocation to address overcrowding in emergency departments.
- **Fire outcomes and response times**: Buffington et al. analyzed fire department response times and their correlation with improved fire outcomes.
- **National EMS delays**: A comprehensive study on EMS delays by the Journal of Emergency Medical Services provided valuable insights into factors contributing to response time delays.

Our project builds upon these works by integrating multiple datasets to provide a comprehensive analysis of emergency response efficiency and incident patterns in San Francisco.

---

## IV. DESIGN AND IMPLEMENTATION

### A. Design Details

![Design Flow](link_to_image)

Our project follows a systematic approach to process and analyze emergency response data from San Francisco. The design consists of multiple stages, from data ingestion to visualization, leveraging big data technologies for efficient processing.

1. **Data Ingestion and Storage**: Three primary datasets from SF.GOV are ingested into HDFS.
2. **Data Processing and Cleaning**: MapReduce jobs process each dataset to remove invalid records, standardize data, and generate clean files.
3. **Data Integration**: Cleaned data is used to create Hive tables and derived tables for analysis.
4. **Analysis and Visualization**: Data is exported to Tableau for visualization and analysis.

### B. Description of Datasets

The datasets are updated daily and were filtered for the last decade, specifically from January 1, 2014, to October 31, 2024.

1. **Fire and EMS Dispatched Calls Dataset**: Contains call number, incident number, call type, response times, and priority level.
2. **Fire Incidents Dataset**: Focuses on fire-related incidents and outcomes, with key fields like incident number, alarm date, and response times.
3. **EMSA Response Times Dataset**: Includes response times for EMS-related incidents and is derived from the Fire Department Calls for Service dataset.

---

## V. RESULTS

### Top Emergency Types

![Top 10 Call Types](link_to_image)

Medical and fire alarms are the most common types of incidents.

### Temporal Trends in Fire and Medical Emergencies

- Yearly trends show a dip in the year 2020-21 due to COVID-19 data exclusion.
- Monthly trends reveal more fire emergencies in winter months due to seasonal factors like heating and holiday decorations.

![Yearly Trend](link_to_image)
![Monthly Trend Fire](link_to_image)
![Monthly Trend Medical](link_to_image)

### Life-Threatening Incidents by Region

The northeast region of San Francisco exhibits a higher proportion of life-threatening incidents, influenced by factors like high population density and limited accessibility.

![Life-Threatening Incidents](link_to_image)

### Average Response Time by Neighborhood

We mapped the average response times for each neighborhood and analyzed resource allocation for high-incident areas.

![Response Time by Neighborhood](link_to_image)

---

## VI. FUTURE WORK

- **Predictive modeling** for emergency demand patterns.
- **Spatial statistics** to identify high-incident areas.
- **Medical emergency patterns** correlation with specific neighborhoods.
- **Real-time data sources** integration for improved decision-making.

---

## VII. CONCLUSION

This study successfully analyzed emergency response data in San Francisco, providing valuable insights into response efficiency and incident patterns. Our findings emphasize the need for better resource allocation, awareness, and planning to improve public safety.

---

## ACKNOWLEDGMENTS

We would like to thank Prof. Yang Tang for his guidance and support throughout the Realtime and Big Data Analytics course, which provided invaluable insights and direction for this project. We would also like to acknowledge NYU High-Performance Computing (HPC) for granting access to computational resources that facilitated efficient data processing and analysis. Further, we are grateful to Tableau for providing a free trial for their software, enabling visualization and analytics for this project.

---

## REFERENCES

1. Dudhale, A., Mishra, A., and Pise, P., 2023. Predictive Model for Emergency Medical Services (EMS) using Machine Learning and Data Mining.
2. Acuna, J.A., Zayas-Castro, J.L., and Charkhgard, H., 2020. Ambulance allocation optimization model for the overcrowding problem in US emergency departments: A case study in Florida. Socio-Economic Planning Sciences, 71, p.100747.
3. Buffington, T., and Ezekoye, O.A., 2019. Statistical analysis of fire department response times and effects on fire outcomes in the United States. Fire technology, 55...
