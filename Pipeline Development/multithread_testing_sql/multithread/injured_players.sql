insert into nba_stats_backup.injured_players(
select player_id, name, team from nba_stats.injuries);
