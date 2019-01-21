insert into nba_stats_backup.team_traditional_boxscore_stats select `b_map`.`game_hash` AS `game_hash`,
                                                           `a`.`TEAM`          AS `team`,
                                                           `a`.`W/L`           AS `W/L`,
                                                           `a`.`MIN`           AS `min`,
                                                           `a`.`PTS`           AS `pts`,
                                                           `a`.`FGM`           AS `fgm`,
                                                           `a`.`FGA`           AS `fga`,
                                                           `a`.`FG%`           AS `FG%`,
                                                           `a`.`3PM`           AS `3pm`,
                                                           `a`.`3PA`           AS `3pa`,
                                                           `a`.`3P%`           AS `3P%`,
                                                           `a`.`FTM`           AS `ftm`,
                                                           `a`.`FTA`           AS `fta`,
                                                           `a`.`FT%`           AS `FT%`,
                                                           `a`.`OREB`          AS `oreb`,
                                                           `a`.`DREB`          AS `dreb`,
                                                           `a`.`REB`           AS `reb`,
                                                           `a`.`AST`           AS `ast`,
                                                           `a`.`TOV`           AS `tov`,
                                                           `a`.`STL`           AS `stl`,
                                                           `a`.`BLK`           AS `blk`,
                                                           `a`.`PF`            AS `pf`,
                                                           `a`.`+/-`           AS `+/-`
                                                    from (`nba_stats`.`box_score_map_view` `b_map`
                                                           join `nba_stats`.`traditional_team_boxscore_stats` `a`
                                                                on (((`b_map`.`home_team` = `a`.`HOME_TEAM`) and
                                                                     (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))))
                                                    union
                                                    select `b_map`.`game_hash` AS `game_hash`,
                                                           `a`.`TEAM`          AS `team`,
                                                           `a`.`W/L`           AS `W/L`,
                                                           `a`.`MIN`           AS `min`,
                                                           `a`.`PTS`           AS `pts`,
                                                           `a`.`FGM`           AS `fgm`,
                                                           `a`.`FGA`           AS `fga`,
                                                           `a`.`FG%`           AS `FG%`,
                                                           `a`.`3PM`           AS `3pm`,
                                                           `a`.`3PA`           AS `3pa`,
                                                           `a`.`3P%`           AS `3P%`,
                                                           `a`.`FTM`           AS `ftm`,
                                                           `a`.`FTA`           AS `fta`,
                                                           `a`.`FT%`           AS `FT%`,
                                                           `a`.`OREB`          AS `oreb`,
                                                           `a`.`DREB`          AS `dreb`,
                                                           `a`.`REB`           AS `reb`,
                                                           `a`.`AST`           AS `ast`,
                                                           `a`.`TOV`           AS `tov`,
                                                           `a`.`STL`           AS `stl`,
                                                           `a`.`BLK`           AS `blk`,
                                                           `a`.`PF`            AS `pf`,
                                                           `a`.`+/-`           AS `+/-`
                                                    from (`nba_stats`.`box_score_map_view` `b_map`
                                                           join `nba_stats`.`traditional_team_boxscore_stats` `a`
                                                                on (((`b_map`.`away_team` = `a`.`AWAY_TEAM`) and
                                                                     (`b_map`.`game_date` = str_to_date(`a`.`GAME_DATE`, '%Y-%m-%d')))));
