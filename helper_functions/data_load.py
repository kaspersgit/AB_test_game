from google.cloud import bigquery

# Loading in data from BigQuery and returning it as a pandas dataframe
def load_data():
    # Importing data
    # query 1
    client = bigquery.Client()

    # Perform query 1
    QUERY = (
        """
        WITH step_1 AS (
              select ass.abtest_group
              , ass.playerid
              , ass.assignment_date
              , DATE_DIFF(CAST(assignment_date AS DATE), CAST(install_date AS DATE), DAY) AS days_pre_assignment
              , SUM(CASE WHEN act.activity_date < ass.assignment_date THEN purchases ELSE 0 END) AS purchases_pre_test 
              , SUM(CASE WHEN act.activity_date < ass.assignment_date THEN gameends ELSE 0 END) AS gamerounds_pre_test 
            FROM `king-ds-recruit-candidate-625.abtest.assignment` ass 
            LEFT JOIN `king-ds-recruit-candidate-625.abtest.activity` act 
              ON act.playerid = ass.playerid 
            GROUP BY 1,2,3,4
        )
        select 
          abtest_group
          , DATE_DIFF(CAST(activity_date AS DATE), CAST(assignment_date AS DATE), DAY) AS days_after_assignment
          , SUM(purchases) AS purchases
          , SUM(gameends) AS gamerounds
          , COUNT(1) AS nr_rows
          , COUNT(DISTINCT s1.playerid) AS unq_players
        FROM step_1 s1 
        LEFT JOIN `king-ds-recruit-candidate-625.abtest.activity` act 
          ON act.playerid = s1.playerid 
        WHERE s1.days_pre_assignment >= 100
          AND s1.purchases_pre_test > 0
          AND s1.gamerounds_pre_test BETWEEN 11 AND 99
        GROUP BY 1,2
        """)
    query_job = client.query(QUERY)  # API request
    test_groups = query_job.result()  # Waits for query to finish

    # Perform query 2
    QUERY = (
        """
        select abtest_group
          , DATE_DIFF(CAST(activity_date AS DATE), CAST(assignment_date AS DATE), DAY) AS days_after_assignment
          , SUM(purchases) AS purchases
          , SUM(gameends) AS gamerounds
          , COUNT(1) AS nr_rows
          , COUNT(DISTINCT ass.playerid) AS unq_players
        FROM `king-ds-recruit-candidate-625.abtest.assignment` ass 
        LEFT JOIN `king-ds-recruit-candidate-625.abtest.activity` act 
          ON act.playerid = ass.playerid 
        GROUP BY 1,2
        """)
    query_job = client.query(QUERY)  # API request
    group_activities = query_job.result()  # Waits for query to finish

    # Perform query 3
    QUERY = (
        """
    select ass.playerid
      , abtest_group
      , DATE_DIFF(CAST(assignment_date AS DATE), CAST(install_date AS DATE), DAY) AS days_pre_assignment
      , DATE_DIFF(CAST('2017-05-23' AS DATE), CAST(assignment_date AS DATE), DAY) AS days_post_assignment
      , SUM(CASE WHEN act.activity_date < ass.assignment_date THEN purchases ELSE 0 END) AS purchases_pre_test 
      , SUM(CASE WHEN act.activity_date >= ass.assignment_date THEN purchases ELSE 0 END) AS purchases_post_test 
      , SUM(CASE WHEN act.activity_date < ass.assignment_date THEN gameends ELSE 0 END) AS gamerounds_pre_test 
      , SUM(CASE WHEN act.activity_date >= ass.assignment_date THEN gameends ELSE 0 END) AS gamerounds_post_test
      , COUNT(DISTINCT CASE WHEN act.activity_date < ass.assignment_date THEN act.activity_date END) AS active_player_days_pre_test 
      , COUNT(DISTINCT CASE WHEN act.activity_date >= ass.assignment_date THEN act.activity_date END) AS active_player_days_post_test
      , COUNT(1) AS nr_rows
      , COUNT(DISTINCT ass.playerid) AS unq_players
    FROM `king-ds-recruit-candidate-625.abtest.assignment` ass 
    LEFT JOIN `king-ds-recruit-candidate-625.abtest.activity` act 
      ON act.playerid = ass.playerid 
    GROUP BY 1,2,3,4
        """)
    query_job = client.query(QUERY)  # API request
    player_lvl = query_job.result()  # Waits for query to finish

    # Convert query result to pandas dataframe
    df_sens = test_groups.to_dataframe(create_bqstorage_client=True)

    df_activities = group_activities.to_dataframe(create_bqstorage_client=True)
    df_players = player_lvl.to_dataframe(create_bqstorage_client=True)

    return df_sens, df_activities, df_players