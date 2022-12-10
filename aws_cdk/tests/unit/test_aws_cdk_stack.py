import aws_cdk as core
import aws_cdk.assertions as assertions

from stacks.storage_layer_stack import StorageLayer

def test_bucket_created():
    app = core.App()
    stack = StorageLayer(app, "aws-cdk-test")
    template = assertions.Template.from_stack(stack)
    template.resource_count_is("AWS::S3::Bucket", 2)
