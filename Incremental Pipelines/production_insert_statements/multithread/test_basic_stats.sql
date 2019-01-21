insert into nba_stats_prod.basic_box_stats(
  select b.game_hash,
         p.player_id,
         b.MP,
         b.FG,
         b.FGA,
         b.FG_PCT,
         b.3P,
         b.3PA,
         b.3P_PCT,
         b.FT,
         b.FT_PCT,
         b.ORB,
         b.DRB,
         b.TRB,
         b.AST,
         b.STL,
         b.BLK,
         b.TOV,
         b.PF,
         b.PTS,
         b.PLUS_MINUS
   from nba_stats.basic_box_stats as b
   inner join nba_stats.player_info_view as p on b.name = p.name
   inner join nba_stats.box_score_map_view as bm on b.game_hash = bm.game_hash);
