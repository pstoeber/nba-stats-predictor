insert into nba_stats_prod.team_advanced_boxscore_stats select `b_map`.`game_hash` AS `game_hash`,
                                                        `a`.`TEAM`          AS `team`,
                                                        `a`.`W/L`           AS `W/L`,
                                                        `a`.`MIN`           AS `min`,
                                                        `a`.`OFFRTG`        AS `OFFRTG`,
                                                        `a`.`DEFRTG`        AS `DEFRTG`,
                                                        `a`.`NETRTG`        AS `NETRTG`,
                                                        `a`.`AST%`          AS `AST%`,
                                                        `a`.`AST/TO`        AS `AST/TO`,
                                                        `a`.`AST_RATIO`     AS `ast_ratio`,
                                                        `a`.`OREB%`         AS `OREB%`,
                                                        `a`.`DREB%`         AS `DREB%`,
                                                        `a`.`REB%`          AS `REB%`,
                                                        `a`.`TOV%`          AS `TOV%`,
                                                        `a`.`EFG%`          AS `EFG%`,
                                                        `a`.`TS%`           AS `TS%`,
                                                        `a`.`PACE`          AS `pace`,
                                                        `a`.`PIE`           AS `PIE`
                                                 from (`nba_stats`.`box_score_map_view` `b_map`
                                                        join `nba_stats`.`advanced_team_boxscore_stats` `a`
                                                             on (((`b_map`.`home_team` = `a`.`TEAM`) and
                                                                  (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                 union
                                                 select `b_map`.`game_hash` AS `game_hash`,
                                                        `a`.`TEAM`          AS `team`,
                                                        `a`.`W/L`           AS `W/L`,
                                                        `a`.`MIN`           AS `min`,
                                                        `a`.`OFFRTG`        AS `OFFRTG`,
                                                        `a`.`DEFRTG`        AS `DEFRTG`,
                                                        `a`.`NETRTG`        AS `NETRTG`,
                                                        `a`.`AST%`          AS `AST%`,
                                                        `a`.`AST/TO`        AS `AST/TO`,
                                                        `a`.`AST_RATIO`     AS `ast_ratio`,
                                                        `a`.`OREB%`         AS `OREB%`,
                                                        `a`.`DREB%`         AS `DREB%`,
                                                        `a`.`REB%`          AS `REB%`,
                                                        `a`.`TOV%`          AS `TOV%`,
                                                        `a`.`EFG%`          AS `EFG%`,
                                                        `a`.`TS%`           AS `TS%`,
                                                        `a`.`PACE`          AS `pace`,
                                                        `a`.`PIE`           AS `PIE`
                                                 from (`nba_stats`.`box_score_map_view` `b_map`
                                                        join `nba_stats`.`advanced_team_boxscore_stats` `a`
                                                             on (((`b_map`.`away_team` = `a`.`TEAM`) and
                                                                  (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))));
