import pandas as pd
import numpy as np

def prepare_data(df_sens_, df_activities_, df_player_):
    df_sens = df_sens_.copy()
    df_activities = df_activities_.copy()
    df_player = df_player_.copy()

    # averaging for activities per day
    df_sens['avg_gamerounds'] = df_sens['gamerounds'] / df_sens['unq_players']
    df_sens['avg_purchases'] = df_sens['purchases'] / df_sens['unq_players']

    df_activities['avg_gamerounds'] = df_activities['gamerounds'] / df_activities['unq_players']
    df_activities['avg_purchases'] = df_activities['purchases'] / df_activities['unq_players']

    # Adding some buckets
    df_player['purchase_lvl'] = np.where(df_player['purchases_pre_test'] == 0, 'low', 'high')

    df_player['engagement_lvl'] = np.where(df_player['gamerounds_pre_test'] < 10, 'low', 'high')
    df_player['engagement_lvl'] = np.where(
        (df_player['gamerounds_pre_test'] > 10) & (df_player['gamerounds_pre_test'] < 100), 'medium',
        df_player['engagement_lvl'])

    df_player['game_age'] = np.where(df_player['days_pre_assignment'] < 10, 'low', 'high')
    df_player['game_age'] = np.where(
        (df_player['days_pre_assignment'] > 10) & (df_player['days_pre_assignment'] < 100), 'medium',
        df_player['game_age'])

    df_player['purchase_made_pre'] = np.where(df_player['purchases_pre_test'] > 0, 1, 0)
    df_player['purchase_made_post'] = np.where(df_player['purchases_post_test'] > 0, 1, 0)

    # averaging for activities per day
    df_player['daily_gamerounds_pre_test'] = df_player['gamerounds_pre_test'] / df_player['days_pre_assignment']
    df_player['daily_purchases_pre_test'] = df_player['purchases_pre_test'] / df_player['days_pre_assignment']

    return df_sens, df_activities, df_player
