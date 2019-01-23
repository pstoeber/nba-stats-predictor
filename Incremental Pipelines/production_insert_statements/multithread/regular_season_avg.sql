insert into nba_stats_prod.RegularSeasonAverages
  select distinct player_id,
                                              season,
                                              team,
                                              gp,
                                              gs,
                                              min,
                                              cast(substr("FGM-A", 1, locate('-', "FGM-A")) as decimal(4, 1)),
                                              cast(substr("FGM-A", (locate('-', "FGM-A") + 1)) as decimal(4, 1)),
                                              "fg%",
                                              cast(substr("3PM-A", 1, locate('-', "FGM-A")) as decimal(4, 1)),
                                              cast(substr("3PM-A", (locate('-', "FGM-A") + 1)) as decimal(4, 1)),
                                              "3p%",
                                              cast(substr("FTM-A", 1, locate('-', "FGM-A")) as decimal(4, 1)),
                                              cast(substr("FTM-A", 1, locate('-', "FGM-A")) as decimal(4, 1)),
                                              "ft%",
                                              "or",
                                              dr,
                                              reb,
                                              ast,
                                              blk,
                                              stl,
                                              pf,
                                              "to",
                                              pts
                              from nba_stats.regularseasonaverages
                              where team != "--" and
                                     player_id in (select player_id from nba_stats.player_info_view);
