
select basic.player_id,
       box_view.team,
       box_view.game_hash,
       box_view.game_date,
       box_view.home_away,
       basic.pts,
       basic.minutes_played,
       basic.fga,
       basic.3pa,
       basic.orb,
       basic.trb,
       basic.ast,
       basic.stl,
       basic.blk,
       basic.tov,
       basic.pf,
       adv.turnover_pct,
       adv.usage_pct,
       a_stats.pace,
       adv.offensive_rating,
       reg_avg.fg_a as tot_fg_a,
       reg_avg.3p_a as tot_3p_a,
       reg_avg.ft_a as tot_ft_a,
       reg_avg.reb as tot_reb,
       reg_avg.ast as tot_ast,
       reg_avg.blk as tot_blk,
       reg_avg.stl as tot_stl,
       reg_avg.pf as tot_pf,
       reg_avg.`TO` as tot_to,
       opp_team_pts.opp_pts,
       opp_team_pts.diff
from (

     select m.game_hash, m.team, m.game_date, m.home_away, lu.season
     from box_scores_map_view as m
            inner join game_date_lookup as lu on m.game_date = lu.day
     where m.game_date < current_date and
           lu.season > 2005 and
           m.home_away = 'Away'
     order by game_date desc

     ) as box_view

<<<<<<< HEAD
inner join(

     select b.game_hash, team.team_id, lu.season
     from box_score_map as b
            inner join game_date_lookup as lu on b.game_date = lu.day
            inner join team_info as team on b.home_team = team.team
     where b.game_date < current_date and
           lu.season > 2005

         ) as home_team on box_view.game_hash = home_team.game_hash

=======
>>>>>>> 8ba48113369b1bdac0c9d683bbc1b210db259795
inner join player_team_map as play_m on ( (box_view.team = play_m.team) and (
box_view.season = play_m.season) )
inner join basic_box_stats as basic on ( (box_view.game_hash = basic.game_hash) and (
play_m.player_id = basic.player_id) )
inner join advanced_box_stats as adv on ( (box_view.game_hash = adv.game_hash) and (
play_m.player_id = adv.player_id) )
inner join team_advanced_boxscore_stats as a_stats on ( (box_view.game_hash = a_stats.game_hash) and (box_view.team = a_stats.team) )
left outer join RegularSeasonAverages as reg_avg on ( (basic.player_id = reg_avg.player_id) and (box_view.season -1 = reg_avg.season) )
<<<<<<< HEAD
inner join points as opp_team_pts on home_team.team_id = opp_team_pts.team_id and home_team.season -1 = opp_team_pts.season
=======
>>>>>>> 8ba48113369b1bdac0c9d683bbc1b210db259795
order by box_view.game_date desc;
