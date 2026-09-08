"""
model-server — the thing that actually holds the model in memory.

Split out from `fraud_prediction_api` on purpose, for two reasons:

1. §15's separation. The API is a request layer plus business logic (feature
   lookup, threshold, drift fan-out); this is the model. Neither one has to know
   how the other is built.
2. §13's champion/challenger. Traffic is split between *models*, not between
   copies of the API. Two Deployments of this service — one resolving the
   `production` alias, one resolving `challenger` — sit behind a single Service,
   and the mesh decides the weights. Nothing about the API changes when a
   challenger is introduced or promoted.
"""
