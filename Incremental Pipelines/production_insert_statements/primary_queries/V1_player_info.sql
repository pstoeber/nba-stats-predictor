 insert into nba_stats_prod.player_info(
  select player_id,
         name,
         experience
  from nba_stats.player_info);
