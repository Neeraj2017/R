/**
 * ============================================================================
 * ========= COPYRIGHT NOTICE
 * ====================================================
 * ================================= Copyright (C) 2015, Helios ITS. All Rights
 * Reserved. Proprietary and confidential. All information contained herein is,
 * and remains the property of Helios ITS . Copying or reproducing the contents
 * of this file, via any medium is strictly prohibited unless prior written
 * permission is obtained from Helios ITS.
 */
package com.heliosits.fusion.workflow.core.service.processor;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hcl.hmtp.common.server.logger.HMTPLogger;
import com.hcl.hmtp.common.server.logger.HMTPLoggerFactory;
import com.heliosits.fusion.base.exception.FusionBaseException;
import com.heliosits.fusion.workflow.WorkflowUtil;
import com.heliosits.fusion.workflow.adapter.client.rest.ProcessInstanceRestClient;
import com.heliosits.fusion.workflow.adapter.dto.ProcessInstanceActionEvent;
import com.heliosits.fusion.workflow.adapter.dto.ProcessInstanceActionRequest;
import com.heliosits.fusion.workflow.adapter.dto.ProcessInstanceCreateEvent;
import com.heliosits.fusion.workflow.adapter.dto.ProcessInstanceCreateRequest;
import com.heliosits.fusion.workflow.adapter.dto.ProcessInstanceResponse;
import com.heliosits.fusion.workflow.adapter.event.service.MessageSender;
import com.heliosits.fusion.workflow.constant.ApplicationConstants;
import com.heliosits.fusion.workflow.exception.config.ErrorCodesConfiguration;
import com.heliosits.fusion.workflow.exception.config.ErrorMessagesConfiguration;

/**
 * Description : It is client interface for handling event messages, it is used
 * for getting the message from message queue and performing workflow related
 * operations.
 * 
 */
@Service
public class ProcessInstanceEventProcessor {

  private final HMTPLogger logger = HMTPLoggerFactory.getLogger(getClass()
      .getSimpleName());

  @Autowired
  private ProcessInstanceRestClient processInstanceRestClient;

  @Autowired
  private MessageSender messageSender;

  @Autowired
  private ErrorCodesConfiguration errorCodesConfiguration;

  @Autowired
  private ErrorMessagesConfiguration errorMessagesConfiguration;

  /**
   * 
   * Message Processor to create process instance.
   * 
   * @param processInstanceCreateEvent
   */
  public void createProcessInstanceProcessor(
      ProcessInstanceCreateEvent processInstanceCreateEvent) {

    logger.info(ApplicationConstants.MESSAGE_RECIEVED
        + processInstanceCreateEvent);

    try {
      // Checking for missing parameters
      StringBuilder errorMessageBuilder =
          validateProcessInstanceCreateEvent(processInstanceCreateEvent);

      if (!errorMessageBuilder.toString().isEmpty()) {

        logger.error(ApplicationConstants.ERROR_IN_MESSAGE_RECIEVED
            + errorMessageBuilder.toString());

        processInstanceCreateEvent.setStatusCode(errorCodesConfiguration
            .getProcessInstancesPostBadRequest());

        // Appending the error messages in the original message event
        processInstanceCreateEvent.setMessage(errorMessageBuilder.toString());

      } else {
        // Process message if there are no errors in the message request
        processProcessInstanceCreateEvent(processInstanceCreateEvent);
      }
    } catch (Exception exception) {
      logger.error(exception.getMessage(), exception);
      processInstanceCreateEvent.setStatusCode(errorCodesConfiguration
          .getInternalServerError());
      processInstanceCreateEvent.setMessage(exception.getMessage());

    }

    // Checking if a acknowledgment is needed
    if (processInstanceCreateEvent.isAcknowledge()
        && !StringUtils.isBlank(processInstanceCreateEvent.getReplyQueue())
        && !StringUtils
            .isBlank(processInstanceCreateEvent.getReplyRoutingkey())) {

      String responseJSon = null;
      try {
        responseJSon = WorkflowUtil.getJsonFromBean(processInstanceCreateEvent);
        messageSender.sendMessage(responseJSon,
            processInstanceCreateEvent.getReplyRoutingkey(),
            processInstanceCreateEvent.getReplyQueue());
      } catch (JsonProcessingException jsonProcessingException) {
        logger.error(jsonProcessingException.getMessage(),
            jsonProcessingException);
      } catch (FusionBaseException fusionBaseException) {
        logger.error(fusionBaseException.getMessage(), fusionBaseException);
      } catch (Exception exception) {
        logger.error(exception.getMessage(), exception);
      }
    }
  }

  /**
   * Validates the ProcessInstanceCreateEvent message
   * 
   * @param processInstanceCreateEvent
   * @return errorMessageBuilder
   */
  private StringBuilder validateProcessInstanceCreateEvent(
      ProcessInstanceCreateEvent processInstanceCreateEvent) {

    StringBuilder errorMessageBuilder = new StringBuilder();

    if (StringUtils.isBlank(processInstanceCreateEvent
        .getProcessDefinitionKey())) {
      errorMessageBuilder.append(ApplicationConstants.ERROR_MESSAGES_SEPARATOR)
          .append(errorMessagesConfiguration.getProcessDefinitionKeyMissing());
    }

    if (StringUtils.isBlank(processInstanceCreateEvent.getTenantName())) {
      errorMessageBuilder.append(ApplicationConstants.ERROR_MESSAGES_SEPARATOR)
          .append(errorMessagesConfiguration.getTenantIdMissing());
    }

    if (processInstanceCreateEvent.isAcknowledge()) {

      if (StringUtils.isBlank(processInstanceCreateEvent.getReplyQueue())) {
        errorMessageBuilder.append(
            ApplicationConstants.ERROR_MESSAGES_SEPARATOR).append(
            errorMessagesConfiguration.getReplyQueueMissing());
      }

      if (StringUtils.isBlank(processInstanceCreateEvent.getReplyRoutingkey())) {
        errorMessageBuilder.append(
            ApplicationConstants.ERROR_MESSAGES_SEPARATOR).append(
            errorMessagesConfiguration.getReplyRoutingKeyMissing());
      }
    }

    return errorMessageBuilder;
  }

  /**
   * Process the message.
   * 
   * @param processInstanceCreateEvent
   */
  private void processProcessInstanceCreateEvent(
      ProcessInstanceCreateEvent processInstanceCreateEvent) {
    ProcessInstanceResponse processInstanceResponse = null;

    ProcessInstanceCreateRequest processInstanceCreateRequest =
        prepareProcessInstanceCreateRequest(processInstanceCreateEvent);

    try {

      // Getting process Instance id from Process Instance Rest Client
      processInstanceResponse =
          processInstanceRestClient
              .createProcessInstance(processInstanceCreateRequest);

      processInstanceCreateEvent
          .setProcessInstanceResponse(processInstanceResponse);

      // If there is no exception, process instance was created successfully.
      processInstanceCreateEvent
          .setStatusCode(ApplicationConstants.STATUS_CODE_201_CREATED);
      processInstanceCreateEvent
          .setMessage(ApplicationConstants.PROCESS_INSTANCE
              + processInstanceResponse.getId()
              + ApplicationConstants.CREATED_SUCCESSFULLY);
    } catch (FusionBaseException fusionBaseException) {
      logger.error(fusionBaseException.getMessage(), fusionBaseException);
      processInstanceCreateEvent.setStatusCode(fusionBaseException
          .getErrorCode());
      processInstanceCreateEvent.setMessage(fusionBaseException
          .getErrorMessage());
    }

  }

  /**
   * Extracts the parameters from ProcessInstanceCreateEvent and set it in
   * ProcessInstanceCreateRequest
   * 
   * @param processInstanceCreateEvent
   * @return ProcessInstanceCreateRequest
   */
  private ProcessInstanceCreateRequest prepareProcessInstanceCreateRequest(
      ProcessInstanceCreateEvent processInstanceCreateEvent) {
    ProcessInstanceCreateRequest processInstanceCreateRequest =
        new ProcessInstanceCreateRequest();
    processInstanceCreateRequest
        .setProcessDefinitionKey(processInstanceCreateEvent
            .getProcessDefinitionKey());
    processInstanceCreateRequest.setTenantId(processInstanceCreateEvent
        .getTenantName());
    processInstanceCreateRequest.setVariables(processInstanceCreateEvent
        .getVariables());
    return processInstanceCreateRequest;
  }

  /**
   * Message Processor to execute process instance actions.
   * 
   * @param processInstanceActionEvent
   */
  public void executeProcessInstanceActionProcessor(
      ProcessInstanceActionEvent processInstanceActionEvent) {

    logger.info(ApplicationConstants.MESSAGE_RECIEVED
        + processInstanceActionEvent);

    try {
      // Checking for missing parameters
      StringBuilder errorMessageBuilder =
          validateProcessInstanceActionEvent(processInstanceActionEvent);

      if (!errorMessageBuilder.toString().isEmpty()) {

        logger.error(ApplicationConstants.ERROR_IN_MESSAGE_RECIEVED
            + errorMessageBuilder.toString());

        processInstanceActionEvent.setStatusCode(errorCodesConfiguration
            .getProcessInstancePutBadRequest());

        // Appending the error messages in the original message event
        processInstanceActionEvent.setMessage(errorMessageBuilder.toString());

      } else {
        // Process message if there are no errors in the message request
        processProcessInstanceActionEvent(processInstanceActionEvent);

      }

    } catch (Exception exception) {
      logger.error(exception.getMessage(), exception);
      processInstanceActionEvent.setStatusCode(errorCodesConfiguration
          .getInternalServerError());
      processInstanceActionEvent.setMessage(exception.getMessage());
    }

    if (processInstanceActionEvent.isAcknowledge()
        && !StringUtils.isBlank(processInstanceActionEvent.getReplyQueue())
        && !StringUtils
            .isBlank(processInstanceActionEvent.getReplyRoutingkey())) {

      String responseJSon = null;
      try {
        responseJSon = WorkflowUtil.getJsonFromBean(processInstanceActionEvent);
        messageSender.sendMessage(responseJSon,
            processInstanceActionEvent.getReplyRoutingkey(),
            processInstanceActionEvent.getReplyQueue());
      } catch (JsonProcessingException jsonProcessingException) {
        logger.error(jsonProcessingException.getMessage(),
            jsonProcessingException);
      } catch (FusionBaseException fusionBaseException) {
        logger.error(fusionBaseException.getMessage(), fusionBaseException);
      } catch (Exception exception) {
        logger.error(exception.getMessage(), exception);
      }
    }
  }

  /**
   * Validates the ProcessInstanceActionEvent message
   * 
   * @param processInstanceActionEvent
   * @return
   */
  private StringBuilder validateProcessInstanceActionEvent(
      ProcessInstanceActionEvent processInstanceActionEvent) {
    StringBuilder errorMessageBuilder = new StringBuilder();

    if (StringUtils.isBlank(processInstanceActionEvent.getProcessInstanceId())) {
      errorMessageBuilder.append(ApplicationConstants.ERROR_MESSAGES_SEPARATOR)
          .append(errorMessagesConfiguration.getProcessInstanceIdMissing());
    }

    if (StringUtils.isBlank(processInstanceActionEvent.getAction())) {
      errorMessageBuilder.append(ApplicationConstants.ERROR_MESSAGES_SEPARATOR)
          .append(errorMessagesConfiguration.getActionMissing());
    }

    if (processInstanceActionEvent.isAcknowledge()) {

      if (StringUtils.isBlank(processInstanceActionEvent.getReplyQueue())) {
        errorMessageBuilder.append(
            ApplicationConstants.ERROR_MESSAGES_SEPARATOR).append(
            errorMessagesConfiguration.getReplyQueueMissing());
      }

      if (StringUtils.isBlank(processInstanceActionEvent.getReplyRoutingkey())) {
        errorMessageBuilder.append(
            ApplicationConstants.ERROR_MESSAGES_SEPARATOR).append(
            errorMessagesConfiguration.getReplyRoutingKeyMissing());
      }
    }
    return errorMessageBuilder;
  }

  /**
   * Process the message.
   * 
   * @param processInstanceActionEvent
   */
  private void processProcessInstanceActionEvent(
      ProcessInstanceActionEvent processInstanceActionEvent) {

    ProcessInstanceActionRequest processInstanceActionRequest =
        prepareProcessInstanceActionRequest(processInstanceActionEvent);
    try {
      processInstanceRestClient.executeProcessInstanceAction(
          processInstanceActionEvent.getProcessInstanceId(),
          processInstanceActionRequest);

      processInstanceActionEvent
          .setStatusCode(ApplicationConstants.STATUS_CODE_200_OK);
      processInstanceActionEvent.setMessage(ApplicationConstants.ACTION1
          + processInstanceActionRequest.getAction()
          + ApplicationConstants.SUCESSFULLY_TAKEN_ON_TASK_ID
          + processInstanceActionEvent.getProcessInstanceId());

    } catch (FusionBaseException fusionBaseException) {
      logger.error(fusionBaseException.getErrorMessage(), fusionBaseException);
      processInstanceActionEvent.setStatusCode(fusionBaseException
          .getErrorCode());
      processInstanceActionEvent.setMessage(fusionBaseException
          .getErrorMessage());
    }
  }

  /**
   * Extracts the parameters from ProcessInstanceActionEvent and set it in
   * ProcessInstanceActionRequest
   * 
   * @param processInstanceActionEvent
   * @return
   */
  private ProcessInstanceActionRequest prepareProcessInstanceActionRequest(
      ProcessInstanceActionEvent processInstanceActionEvent) {

    ProcessInstanceActionRequest processInstanceActionRequest =
        new ProcessInstanceActionRequest();

    processInstanceActionRequest.setAction(processInstanceActionEvent
        .getAction());

    return processInstanceActionRequest;
  }



}
