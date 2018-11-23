
select preds.game_hash,
       preds.team,
       preds.win_probability,
       preds.lose_probability,
       preds.team_score,
       preds.opp_score,
       (case
          when preds.team_score > preds.opp_score then 1
          else 0 end) as flag
from win_predictions_comparison_view as preds
