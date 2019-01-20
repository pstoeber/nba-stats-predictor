insert into nba_stats_backup.advanced_box_stats(
  select a.game_hash,
         p.player_id,
         a.MP,
         a.TS_PCT,
         a.EFG_PCT,
         a.3PAR,
         a.FTR,
         a.ORB_PCT,
         a.DRB_PCT,
         a.TRB_PCT,
         a.AST_PCT,
         a.STL_PCT,
         a.BLK_PCT,
         a.TOV_PCT,
         a.USG_PCT,
         a.ORTG,
         a.DRTG
   from nba_stats.advanced_box_stats as a
   inner join nba_stats.player_info_view as p on a.name = p.name
   inner join nba_stats.box_score_map_view as bm on a.game_hash = bm.game_hash);
