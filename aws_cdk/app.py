#!/usr/bin/env python3
import aws_cdk as cdk

from stacks.storage_layer_stack import StorageLayer

app = cdk.App()
StorageLayer(
    app, 
    "AwsCdkStack"
    )

app.synth()
