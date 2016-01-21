/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.openmrs.module.muzimaregistration.handler;

import net.minidev.json.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.Location;
import org.openmrs.Patient;
import org.openmrs.PatientIdentifier;
import org.openmrs.PatientIdentifierType;
import org.openmrs.PersonAddress;
import org.openmrs.PersonAttribute;
import org.openmrs.PersonAttributeType;
import org.openmrs.PersonName;
import org.openmrs.User;
import org.openmrs.annotation.Handler;
import org.openmrs.api.PersonService;
import org.openmrs.api.context.Context;
import org.openmrs.module.muzima.exception.QueueProcessorException;
import org.openmrs.module.muzima.model.QueueData;
import org.openmrs.module.muzima.model.handler.QueueDataHandler;
import org.openmrs.module.muzimaregistration.utils.JsonUtils;
import org.openmrs.module.muzimaregistration.utils.PatientSearchUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
@Component
@Handler(supports = QueueData.class, order = 6)
public class DemographicsUpdateQueueDataHandler implements QueueDataHandler {

    private static final String DISCRIMINATOR_VALUE = "json-demographics-update";

    private final Log log = LogFactory.getLog(DemographicsUpdateQueueDataHandler.class);

    private Patient unsavedPatient;
    private Patient savedPatient;
    private String payload;
    private QueueProcessorException queueProcessorException;

    @Override
    public void process(final QueueData queueData) throws QueueProcessorException {
        log.info("Processing demographics update form data: " + queueData.getUuid());
        try {
            if (validate(queueData)) {
                updateSavedPatientDemographics();
                Context.getPatientService().savePatient(savedPatient);
            }
        } catch (Exception e) {
            if (!e.getClass().equals(QueueProcessorException.class)) {
                queueProcessorException.addException(e);
            }
        } finally {
            if (queueProcessorException.anyExceptions()) {
                throw queueProcessorException;
            }
        }
    }

    private void updateSavedPatientDemographics(){
        if(unsavedPatient.getIdentifiers() != null){
            savedPatient.addIdentifiers(unsavedPatient.getIdentifiers());
        }
        if(unsavedPatient.getPersonName() != null) {
            savedPatient.addName(unsavedPatient.getPersonName());
        }
        if(StringUtils.isNotBlank(unsavedPatient.getGender())) {
            savedPatient.setGender(unsavedPatient.getGender());
        }
        if(unsavedPatient.getBirthdate() != null) {
            savedPatient.setBirthdate(unsavedPatient.getBirthdate());
            savedPatient.setBirthdateEstimated(unsavedPatient.getBirthdateEstimated());
        }
        if(unsavedPatient.getPersonAddress() != null) {
            savedPatient.addAddress(unsavedPatient.getPersonAddress());
        }
        if(unsavedPatient.getAttributes() != null) {
            Set<PersonAttribute> attributes = unsavedPatient.getAttributes();
            Iterator<PersonAttribute> iterator = attributes.iterator();
            while(iterator.hasNext()) {
                savedPatient.addAttribute(iterator.next());
            }
        }
        if(unsavedPatient.getChangedBy() != null) {
            savedPatient.setChangedBy(unsavedPatient.getChangedBy());
        }
    }

    @Override
    public boolean validate(QueueData queueData) {
        log.info("Processing demographics Update form data: " + queueData.getUuid());
        queueProcessorException = new QueueProcessorException();
        try {
            payload = queueData.getPayload();
            Patient candidatePatient = getCandidatePatientFromPayload();
            savedPatient = PatientSearchUtils.findSavedPatient(candidatePatient,true);
            if(savedPatient == null){
                queueProcessorException.addException(new Exception("Unable to uniquely identify patient for this " +
                        "demographic update form data. "));
            } else {
                unsavedPatient = new Patient();
                populateUnsavedPatientDemographicsFromPayload();
            }
            return true;
        } catch (Exception e) {
            queueProcessorException.addException(e);
            return false;
        } finally {
            if (queueProcessorException.anyExceptions()) {
                throw queueProcessorException;
            }
        }
    }

    @Override
    public String getDiscriminator() {
        return DISCRIMINATOR_VALUE;
    }

    private Patient getCandidatePatientFromPayload(){
        Patient candidatePatient = new Patient();

        String uuid = getCandidatePatientUuidFromPayload();
        candidatePatient.setUuid(uuid);

        PatientIdentifier preferredIdentifier = getCandidatePatientPreferredIdentifierFromPayload();
        candidatePatient.addIdentifier(preferredIdentifier);

        PersonName personName = getCandidatePatientPersonNameFromPayload();
        candidatePatient.addName(personName);

        String gender = getCandidatePatientGenderFromPayload();
        candidatePatient.setGender(gender);

        Date birthDate = getCandidatePatientBirthDateFromPayload();
        candidatePatient.setBirthdate(birthDate);

        return candidatePatient;
    }

    private String getCandidatePatientUuidFromPayload(){
        return JsonUtils.readAsString(payload, "$['patient']['patient.uuid']");
    }

    private PatientIdentifier getCandidatePatientPreferredIdentifierFromPayload(){
        String identifierValue = JsonUtils.readAsString(payload, "$['patient']['patient.medical_record_number']");
        String identifierTypeName = "AMRS Universal ID";

        PatientIdentifier preferredPatientIdentifier = createPatientIdentifier(identifierTypeName, identifierValue);
        if (preferredPatientIdentifier != null) {
            preferredPatientIdentifier.setPreferred(true);
        }
        return preferredPatientIdentifier;
    }

    private PersonName getCandidatePatientPersonNameFromPayload(){
        PersonName personName = new PersonName();
        String givenName = JsonUtils.readAsString(payload, "$['patient']['patient.given_name']");
        if(StringUtils.isNotBlank(givenName)){
            personName.setGivenName(givenName);
        }
        String familyName = JsonUtils.readAsString(payload, "$['patient']['patient.family_name']");
        if(StringUtils.isNotBlank(familyName)){
            personName.setFamilyName(familyName);
        }

        String middleName= JsonUtils.readAsString(payload, "$['patient']['patient.middle_name']");
        if(StringUtils.isNotBlank(middleName)){
            personName.setMiddleName(middleName);
        }

        return personName;

    }

    private String getCandidatePatientGenderFromPayload(){
        return JsonUtils.readAsString(payload, "$['patient']['patient.sex']");
    }

    private Date getCandidatePatientBirthDateFromPayload(){
        return JsonUtils.readAsDate(payload, "$['patient']['patient.birth_date']");
    }

    private void populateUnsavedPatientDemographicsFromPayload() {
        setUnsavedPatientIdentifiersFromPayload();
        setUnsavedPatientBirthDateFromPayload();
        setUnsavedPatientBirthDateEstimatedFromPayload();
        setUnsavedPatientGenderFromPayload();
        setUnsavedPatientNameFromPayload();
        setUnsavedPatientAddressesFromPayload();
        setUnsavedPatientPersonAttributesFromPayload();
        setUnsavedPatientChangedByFromPayload();
    }

    private void setUnsavedPatientIdentifiersFromPayload() {
        List<PatientIdentifier> otherIdentifiers = getOtherPatientIdentifiersFromPayload();
        if (!otherIdentifiers.isEmpty()) {
            Set<PatientIdentifier> patientIdentifiers = new HashSet<PatientIdentifier>();
            patientIdentifiers.addAll(otherIdentifiers);
            setIdentifierTypeLocation(patientIdentifiers);
            unsavedPatient.addIdentifiers(patientIdentifiers);
        }
    }

    private List<PatientIdentifier> getOtherPatientIdentifiersFromPayload() {
        List<PatientIdentifier> otherIdentifiers = new ArrayList<PatientIdentifier>();
        Object identifierTypeNameObject = JsonUtils.readAsObject(payload,
                "$['demographicsupdate']['demographicsupdate.other_identifier_type']");
        Object identifierValueObject =JsonUtils.readAsObject(payload,
                "$['demographicsupdate']['demographicsupdate.other_identifier_value']");

        if (identifierTypeNameObject instanceof JSONArray) {
            JSONArray identifierTypeName = (JSONArray) identifierTypeNameObject;
            JSONArray identifierValue = (JSONArray) identifierValueObject;
            for (int i = 0; i < identifierTypeName.size(); i++) {
                PatientIdentifier identifier = createPatientIdentifier(identifierTypeName.get(i).toString(),
                        identifierValue.get(i).toString());
                if (identifier != null) {
                    otherIdentifiers.add(identifier);
                }
            }
        } else if (identifierTypeNameObject instanceof String) {
            String identifierTypeName = (String) identifierTypeNameObject;
            String identifierValue = (String) identifierValueObject;
            PatientIdentifier identifier = createPatientIdentifier(identifierTypeName, identifierValue);
            if (identifier != null) {
                otherIdentifiers.add(identifier);
            }
        }
        return otherIdentifiers;
    }

    private PatientIdentifier createPatientIdentifier(String identifierTypeName, String identifierValue) {
        PatientIdentifierType identifierType = Context.getPatientService()
                .getPatientIdentifierTypeByName(identifierTypeName);
        if (identifierType == null) {
            queueProcessorException.addException(
                    new Exception("Unable to find identifier type with name: " + identifierTypeName));
        } else if (identifierValue == null) {
            queueProcessorException.addException(
                    new Exception("Identifier value can't be null type: " + identifierTypeName));
        } else {
            PatientIdentifier patientIdentifier = new PatientIdentifier();
            patientIdentifier.setIdentifierType(identifierType);
            patientIdentifier.setIdentifier(identifierValue);
            return patientIdentifier;
        }
        return null;
    }

    private void setIdentifierTypeLocation(final Set<PatientIdentifier> patientIdentifiers) {
        Location location = getLocationFromPayload();

        if (location == null) {
            String locationIdString = JsonUtils.readAsString(payload, "$['encounter']['encounter.location_id']");
            queueProcessorException.addException(
                    new Exception("Unable to find encounter location using the id: " + locationIdString));
        } else {
            Iterator<PatientIdentifier> iterator = patientIdentifiers.iterator();
            while (iterator.hasNext()) {
                PatientIdentifier identifier = iterator.next();
                identifier.setLocation(location);
            }
        }
    }

    private Location getLocationFromPayload(){
        String locationIdString = JsonUtils.readAsString(payload, "$['encounter']['encounter.location_id']");
        Location location = null;
        int locationId;

        if(locationIdString != null){
            locationId = Integer.parseInt(locationIdString);
            location = Context.getLocationService().getLocation(locationId);
        }
        return location;
    }

    private void setUnsavedPatientBirthDateFromPayload(){
        Date birthDate = JsonUtils.readAsDate(payload, "$['demographicsupdate']['demographicsupdate.birth_date']");
        if(birthDate != null){
            if(isBirthDateChangeValidated()){
                unsavedPatient.setBirthdate(birthDate);
            }else{
                queueProcessorException.addException(
                        new Exception("Change of Birth Date requires manual review"));
            }
        }

    }

    private void setUnsavedPatientBirthDateEstimatedFromPayload(){
        boolean birthdateEstimated = JsonUtils.readAsBoolean(payload,
                "$['demographicsupdate']['demographicsupdate.birthdate_estimated']");
        unsavedPatient.setBirthdateEstimated(birthdateEstimated);
    }

    private void setUnsavedPatientGenderFromPayload(){
        String gender = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.sex']");
        if(StringUtils.isNotBlank(gender)){
            if(isGenderChangeValidated()){
                unsavedPatient.setGender(gender);
            }else{
                queueProcessorException.addException(
                        new Exception("Change of Gender requires manual review"));
            }
        }
    }

    private void setUnsavedPatientNameFromPayload(){
        PersonName personName = new PersonName();
        String givenName = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.given_name']");
        if(StringUtils.isNotBlank(givenName)){
            personName.setGivenName(givenName);
        }
        String familyName = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.family_name']");
        if(StringUtils.isNotBlank(familyName)){
            personName.setFamilyName(familyName);
        }

        String middleName= JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.middle_name']");
        if(StringUtils.isNotBlank(middleName)){
            personName.setMiddleName(middleName);
        }

        if(StringUtils.isNotBlank(personName.getFullName())) {
            unsavedPatient.addName(personName);
        }
    }

    private void setUnsavedPatientAddressesFromPayload(){
        PersonAddress patientAddress = new PersonAddress();

        String county = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.county']");
        if(StringUtils.isNotBlank(county)) {
            patientAddress.setStateProvince(county);
        }

        String location = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.location']");
        if(StringUtils.isNotBlank(location)) {
            patientAddress.setAddress6(location);
        }

        String sub_location = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.sub_location']");
        if(StringUtils.isNotBlank(sub_location)) {
            patientAddress.setAddress5(sub_location);
        }

        String village = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.village']");
        if(StringUtils.isNotBlank(village)) {
            patientAddress.setCityVillage(village);
        }
        if(!patientAddress.isBlank()){
            unsavedPatient.addAddress(patientAddress);
        }
    }

    private void setUnsavedPatientPersonAttributesFromPayload(){
        String mothersName = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.mothers_name']");
        if(StringUtils.isNotBlank(mothersName)) {
            setAsAttribute("Mother's Name", mothersName);
        }

        String phoneNumber = JsonUtils.readAsString(payload, "$['demographicsupdate']['demographicsupdate.phone_number']");
        if(StringUtils.isNotBlank(phoneNumber)) {
            setAsAttribute("Contact Phone Number", phoneNumber);
        }
    }

    private void setAsAttribute(String attributeTypeName, String value){
        PersonService personService = Context.getPersonService();
        PersonAttributeType attributeType = personService.getPersonAttributeTypeByName(attributeTypeName);
        if(attributeType !=null && StringUtils.isNotBlank(value)){
            PersonAttribute personAttribute = new PersonAttribute(attributeType, value);
            unsavedPatient.addAttribute(personAttribute);
        } else if(attributeType ==null){
            queueProcessorException.addException(
                    new Exception("Unable to find Person Attribute type by name '" + attributeTypeName + "'")
            );
        }
    }

    private  void setUnsavedPatientChangedByFromPayload(){
        String providerString = JsonUtils.readAsString(payload, "$['encounter']['encounter.provider_id']");
        User user = Context.getUserService().getUserByUsername(providerString);
        if (user == null) {
            queueProcessorException.addException(new Exception("Unable to find user using the id: " + providerString));
        } else {
            unsavedPatient.setChangedBy(user);
        }
    }

    private boolean isBirthDateChangeValidated(){
        return JsonUtils.readAsBoolean(payload, "$['demographicsupdate']['demographicsupdate.birthdate_change_validated']");
    }

    private boolean isGenderChangeValidated(){
        return JsonUtils.readAsBoolean(payload, "$['demographicsupdate']['demographicsupdate.gender_change_validated']");
    }

    @Override
    public boolean accept(final QueueData queueData) {
        return StringUtils.equals(DISCRIMINATOR_VALUE, queueData.getDiscriminator());
    }
}