insert into nba_stats_backup.player_usage_stats (select `b`.`game_hash`  AS `game_hash`,
                                               `pv`.`player_id` AS `player_id`,
                                               `u`.`USG%`       AS `usg%`,
                                               `u`.`%FGM`       AS `%fgm`,
                                               `u`.`%FGA`       AS `%fga`,
                                               `u`.`%3PM`       AS `%3pm`,
                                               `u`.`%3PA`       AS `%3pa`,
                                               `u`.`%FTM`       AS `%ftm`,
                                               `u`.`%FTA`       AS `%fta`,
                                               `u`.`%OREB`      AS `%oreb`,
                                               `u`.`%DREB`      AS `%dreb`,
                                               `u`.`%REB`       AS `%reb`,
                                               `u`.`%AST`       AS `%ast`,
                                               `u`.`%TOV`       AS `%tov`,
                                               `u`.`%STL`       AS `%stl`,
                                               `u`.`%BLK`       AS `%blk`,
                                               `u`.`%BLKA`      AS `%blka`,
                                               `u`.`%PF`        AS `%pf`,
                                               `u`.`%PFD`       AS `%pfd`,
                                               `u`.`%PTS`       AS `%pts`
                                        from (((((select `box_score_map_view`.`game_hash` AS `game_hash`,
                                                         `box_score_map_view`.`home_team` AS `team`,
                                                         `box_score_map_view`.`game_date` AS `game_date`
                                                  from `nba_stats`.`box_score_map_view`
                                                  union
                                                  select `box_score_map_view`.`game_hash` AS `game_hash`,
                                                         `box_score_map_view`.`away_team` AS `team`,
                                                         `box_score_map_view`.`game_date` AS `game_date`
                                                  from `nba_stats`.`box_score_map_view`) `b` join `nba_stats`.`game_date_lookup` `lu` on ((`b`.`game_date` = `lu`.`day`))) join `nba_stats`.`player_team_map_view` `pv` on (((`lu`.`season` = `pv`.`season`) and (`b`.`team` = `pv`.`team`)))) join `nba_stats`.`player_info` `p` on ((`pv`.`player_id` = `p`.`player_id`)))
                                               join `nba_stats`.`player_usage_stats` `u`
                                                    on (((`b`.`team` = `u`.`team`) and (`p`.`NAME` = `u`.`name`) and
                                                         (`b`.`game_date` = `u`.`game_date`)))));
