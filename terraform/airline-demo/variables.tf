# airline-demo takes no credentials of its own — everything is inherited from core
# via terraform_remote_state. The knobs are the workshop, whether the recovery agent
# runs, and always-on mode. Tableflow's knobs live in terraform/tableflow.

variable "workshop" {
  description = "Deploy the self-service workshop's starting point (docs/flight-disruptions/workshop.md): the four source feeds (flights, itineraries, hotels, flight telemetry) and passenger_recommendations only. Attendees create passenger_state, flight_impact, and the recovery agent in the SQL workspace, and the workshop leaves out Tableflow and the hotel webhook. `uv run deploy --workshop` sets it."
  type        = bool
  default     = false
}

variable "enable_recovery_agent" {
  description = "Run the recovery agent (sql/26-32) on Claude hosted in Confluent, which writes the live offers for RA417, RA183, and RA185 in place of the generator (every flight when var.continuous is set). Needs the 'rtce-connection' MCP connection; `uv run deploy` creates it after enabling RTCE, then sets this with a targeted apply."
  type        = bool
  default     = false
}

variable "continuous" {
  description = "Always-on mode: the recovery agent handles every flight, and the continuous generator writes no live offers."
  type        = bool
  default     = false
}
