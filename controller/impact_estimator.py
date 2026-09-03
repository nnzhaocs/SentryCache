# Impact estimator + event serializer (P3, paper §V-E).
# Classifies incoming topology events into Minor / Moderate / Severe via theta_lo / theta_hi,
# and serializes Moderate+Severe events behind any in-progress event until phi_r >= theta_r.
