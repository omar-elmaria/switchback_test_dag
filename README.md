# switchback_test_dag
This repo contains a data pipeline composed of Python and Big Query scripts that extract, clean, and aggregate **switchback test** data, as well as perform statistical 
significance tests. The code is fully orchestrated on **Airflow** and feeds a **Tableau dashboard** that displays the success metrics of switchback experiments.

# Introduction to Switchback Testing
**Switchback testing** is an experimentation framework that is used when independence between the variation and control groups cannot be established through the standard way of running AB tests. If you are not familiar with AB testing, the typical way you could create two groups that are independent of one another is by **splitting traffic evenly** across the number of variants. The problem with establishing independence happens when the intervention being tested affects part of the ecosystem that is **shared** among the variation and control groups.

In the world of E-commerce and 2/3-sided marketplace businesses, switchback testing is commonly used to test new **surge pricing setups** or **dispatch algorithms**. These are typical use cases because the interventions tested in these types of experiments usually affect one side of the marketplace that is **coupled** with the customers in the variation and control groups. These customers are the part of the ecosystem that ultimately **react** to the stimuli you expose them to and **produce** signals in the data that you can collect and measure.

An example of that coupling effect is a surge pricing setup in an **online delivery marketplace** that aims to curb the demand of customers through increased delivery fees during peak demand hours. This strategy is often used in platform-type businesses (e.g., Uber, Lyft, Doordash) to allow enough time for the supply side (i.e., riders/drivers fleet) to catch up with demand, thereby gradually moving the ecosystem back to an **equilibrium state**. This dynamic gives rise to a classic problem called _**Network Effects**_.

## What are Network Effects?
Network effects can be explained through the infographic below

![image](https://user-images.githubusercontent.com/98691360/193418293-45709e62-d0f0-4b85-b6e5-2605442e41d4.png)

The problem here is that the fleet would be able to serve customers that paid no surge fees "faster". Why, you might ask? It is because the surge fees paid by the group that was exposed to surge would **indirectly** impact the group that was not subjected to this treatment. Assuming our main success metric is reducing **delivery delays**, the delays associated with the group that did **not** get exposed to surge would be **"polluted"** by the spillover effect from the other group. It doesn't matter how you aggregate the KPIs at the end. The delivery delays of orders placed by the non-surged group would likely be **underestimated** due to the coupling effect caused by the **shared fleet** between the two groups of customers.

Doordash data scientists wrote a very nice [article](https://medium.com/@DoorDash/switchback-tests-and-randomized-experimentation-under-network-effects-at-doordash-f1d938ab7c2a) about switchback testing, when to use it, and how to analyze it. I encourage you to read it to get more familiarized with the concept.

# Main Objective of the Project
The code in this repo is intended to automate the process of extracting, cleaning, and aggregating switchback test data, in addition to performing statistical tests to
determine if the deltas between the variation and control groups in the test are statistically significant. The code outputs a few tables that feed a Tableau dashboard
showcasing the important KPIs that are tracked in surge pricing tests. A glimpse of the dashboard is shown below.
![image](https://user-images.githubusercontent.com/98691360/193418690-176a72e1-2e7a-4410-a9c9-ab3346c15ae1.png)

# Secondary Objective of the Project
The project also inluded a research-based part to determine the best practices of switchback testing to be adopted at scale in pricing use cases at my company. The **notebooks** section of the project aimed to answer questions such as:
- Should we **randomize the assignment of the treatment**?
- If yes, what is the best **switchback window size** to use? Is it 1 day, 12 hours, 2 hours, or what?
- How long does it take for the effect of a new pricing setup to **effectively propagate through the ecosystem** such that customers and riders start reacting to it
and sending detectable signals in the test data?

# Main Findings
The primary conclusions of the research part of the project were as follows:
1. Randomization is required to eliminate bias in the experiment and mimic a perfect randomized control trial. It should be done by **time of day**, **day of week**, and **geographical zones/regions**

The infographic below visualizes what this means (courtesy of this Doordash [article](https://medium.com/@DoorDash/switchback-tests-and-randomized-experimentation-under-network-effects-at-doordash-f1d938ab7c2a))
![image](https://user-images.githubusercontent.com/98691360/193418970-a8c9fc90-17b9-41cb-8947-ecf128607d2a.png)

2. The best switchback window size is *1 hour*
3. The effect of a new pricing setup is typically **_felt_** by the ecosystem of customers and riders **after 1 to 2 hours** from switching to a new pricing configuration

# Usability and Reproducability
This project was an **internal project** and used proprietary data sources and analysis methodologies. Even though you can clone the repo, the results **cannot** 
be reproduced on another machine due to data sharing restrictions. The code will simply give you an error because you don't have the necessary data access permissions. 

That said, if you are interested in knowing more about the framework or leveraging it in a particular use case, feel free to contact me on [LinkedIn](https://www.linkedin.com/in/omar-elmaria/).
