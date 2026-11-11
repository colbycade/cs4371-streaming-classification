# How to Create and Export the Top 10 Entities by Count Dashboard
Just in case we ever need to do this again.

1. Navigate to http://localhost:5601.
2. Open the **hamburger menu** and navigate to **Management → Stack Management → Kibana → Data Views**. Click **Create data view**.
3. Create a data view with the following settings:
   - **Name:** `entity-counts`
   - **Index pattern:** `entity-counts`
   - **Timestamp field:** **I don't want to use the time filter**
   Click **Save data view to Kibana**.
4. Open the **hamburger menu** and navigate to **Analytics → Dashboards**. Click **Create dashboard**, then **Create visualization**. Select **Bar chart** if needed.
5. Drag `entity.keyword` to the **Horizontal axis**, then click it to configure:
   1. Set **Number of values** to **10**.
   2. Change **Rank by** to **Custom**.
   3. Set **Rank function** to **Maximum**.
   4. Click **Records** and select **count**.
   5. Expand **Advanced** and disable **Group remaining values as "Other"**.
   6. Rename the axis to **Top 10 Named Entities**.
   7. Leave **Sort** set to **Descending** and click **Close**.
6. Drag `count` to the **Vertical axis**, then click it to configure:
   1. Under **Functions**, select **Maximum**.
   2. Rename the axis to **Count**.
7. Click **Save and return** to return to the dashboard.
8. Click **Save** and name the dashboard **Top 10 Named Entities**.
9. Enable auto-refresh:
   1. Click the **calendar icon** in the upper-right dashboard toolbar.
   2. Enable **Refresh every**.
   3. Set the interval to **10 seconds**.
   4. Click **Apply**.
   5. Click **Settings** from the top menu.
   6. Toggle on **Store time with dashboard** to save the autorefresh settings.
10. Export the dashboard:
    1. Navigate to **Management → Stack Management → Kibana → Saved Objects**.
    2. Select **Top 10 Named Entities** and click **Export**.
    3. Enable **Include related objects**.
    4. Click **Export** and save the downloaded file as `results/kibana-dashboard.ndjson`.