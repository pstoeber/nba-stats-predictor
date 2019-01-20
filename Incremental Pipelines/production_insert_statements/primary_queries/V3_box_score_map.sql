insert into nba_stats_prod.box_score_map(
  select box_m.game_hash,
       box_m.away_team,
       box_m.home_team,
       box_m.game_date
 from nba_stats.box_score_map as box_m
 where box_m.game_hash not in (select game_hash from nba_stats.do_not_include_hashes) and
       box_m.away_team in (select team from nba_stats.team_info) and
       box_m.home_team in (select team from nba_stats.team_info));
