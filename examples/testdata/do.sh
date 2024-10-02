#!/bin/bash
time grpcurl -plaintext -format json -format-error -d @ localhost:8205 proto.Customer/OnboardCustomer <onboard-request.json
