# LocalStack Resource Provider Scaffolding v2
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, TypedDict

import localstack.services.cloudformation.provider_utils as util
from localstack.services.cloudformation.resource_provider import (
    OperationStatus,
    ProgressEvent,
    ResourceProvider,
    ResourceRequest,
)
from localstack.utils.strings import canonicalize_bool_to_str, short_uid


class SNSTopicProperties(TypedDict):
    ContentBasedDeduplication: Optional[bool]
    DataProtectionPolicy: Optional[dict]
    DisplayName: Optional[str]
    FifoTopic: Optional[bool]
    KmsMasterKeyId: Optional[str]
    SignatureVersion: Optional[str]
    Subscription: Optional[list[Subscription]]
    Tags: Optional[list[Tag]]
    TopicArn: Optional[str]
    TopicName: Optional[str]
    TracingConfig: Optional[str]


class Subscription(TypedDict):
    Endpoint: Optional[str]
    Protocol: Optional[str]


class Tag(TypedDict):
    Key: Optional[str]
    Value: Optional[str]


REPEATED_INVOCATION = "repeated_invocation"


class SNSTopicProvider(ResourceProvider[SNSTopicProperties]):
    TYPE = "AWS::SNS::Topic"  # Autogenerated. Don't change
    SCHEMA = util.get_schema_path(Path(__file__))  # Autogenerated. Don't change

    def create(
        self,
        request: ResourceRequest[SNSTopicProperties],
    ) -> ProgressEvent[SNSTopicProperties]:
        """
        Create a new resource.

        Primary identifier fields:
          - /properties/TopicArn



        Create-only properties:
          - /properties/TopicName
          - /properties/FifoTopic

        Read-only properties:
          - /properties/TopicArn

        IAM permissions required:
          - sns:CreateTopic
          - sns:TagResource
          - sns:Subscribe
          - sns:GetTopicAttributes
          - sns:PutDataProtectionPolicy

        """
        model = request.desired_state
        sns = request.aws_client_factory.sns

        attributes = {
            k: v
            for k, v in model.items()
            if v is not None
            if k not in ["TopicName", "Subscription", "Tags"]
        }
        if (fifo_topic := attributes.get("FifoTopic")) is not None:
            attributes["FifoTopic"] = canonicalize_bool_to_str(fifo_topic)

        if archive_policy := attributes.get("ArchivePolicy"):
            archive_policy["MessageRetentionPeriod"] = str(archive_policy["MessageRetentionPeriod"])
            attributes["ArchivePolicy"] = json.dumps(archive_policy)

        if (content_based_dedup := attributes.get("ContentBasedDeduplication")) is not None:
            attributes["ContentBasedDeduplication"] = canonicalize_bool_to_str(content_based_dedup)

        # Default name
        if model.get("TopicName") is None:
            model["TopicName"] = (
                f"topic-{short_uid()}.fifo" if fifo_topic else f"topic-{short_uid()}"
            )

        create_sns_response = sns.create_topic(Name=model["TopicName"], Attributes=attributes)
        model["TopicArn"] = create_sns_response["TopicArn"]

        # now we add subscriptions if they exists
        for subscription in model.get("Subscription", []):
            sns.subscribe(
                TopicArn=model["TopicArn"],
                Protocol=subscription["Protocol"],
                Endpoint=subscription["Endpoint"],
            )
        if tags := model.get("Tags"):
            sns.tag_resource(ResourceArn=model["TopicArn"], Tags=tags)

        return ProgressEvent(
            status=OperationStatus.SUCCESS,
            resource_model=model,
            custom_context=request.custom_context,
        )

    def read(
        self,
        request: ResourceRequest[SNSTopicProperties],
    ) -> ProgressEvent[SNSTopicProperties]:
        """
        Fetch resource information

        IAM permissions required:
          - sns:GetTopicAttributes
          - sns:ListTagsForResource
          - sns:ListSubscriptionsByTopic
          - sns:GetDataProtectionPolicy
        """
        raise NotImplementedError

    def delete(
        self,
        request: ResourceRequest[SNSTopicProperties],
    ) -> ProgressEvent[SNSTopicProperties]:
        """
        Delete a resource

        IAM permissions required:
          - sns:DeleteTopic
        """
        model = request.desired_state
        sns = request.aws_client_factory.sns
        sns.delete_topic(TopicArn=model["TopicArn"])
        return ProgressEvent(status=OperationStatus.SUCCESS, resource_model={})

    def update(
        self,
        request: ResourceRequest[SNSTopicProperties],
    ) -> ProgressEvent[SNSTopicProperties]:
        """
        Update a resource

        IAM permissions required:
          - sns:SetTopicAttributes
          - sns:TagResource
          - sns:UntagResource
          - sns:Subscribe
          - sns:Unsubscribe
          - sns:GetTopicAttributes
          - sns:ListTagsForResource
          - sns:ListSubscriptionsByTopic
          - sns:GetDataProtectionPolicy
          - sns:PutDataProtectionPolicy
        """
        raise NotImplementedError
