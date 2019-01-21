 insert into nba_stats_backup.player_info(
  select player_id,
         name,
         experience
  from nba_stats.player_info);
