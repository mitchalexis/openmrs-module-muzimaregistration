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
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.Concept;
import org.openmrs.Location;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.PatientIdentifier;
import org.openmrs.PatientIdentifierType;
import org.openmrs.PersonAddress;
import org.openmrs.PersonAttribute;
import org.openmrs.PersonAttributeType;
import org.openmrs.PersonName;
import org.openmrs.annotation.Handler;
import org.openmrs.api.PersonService;
import org.openmrs.api.context.Context;
import org.openmrs.module.kenyaemr.Dictionary;
import org.openmrs.module.muzima.exception.QueueProcessorException;
import org.openmrs.module.muzima.model.QueueData;
import org.openmrs.module.muzima.model.handler.QueueDataHandler;
import org.openmrs.module.muzimaregistration.api.RegistrationDataService;
import org.openmrs.module.muzimaregistration.api.model.RegistrationData;
import org.openmrs.module.muzimaregistration.utils.JsonUtils;
import org.openmrs.module.kenyaemr.metadata.CommonMetadata;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * TODO: Write brief description about the class here.
 */
@Handler(supports = QueueData.class, order = 1)
public class JsonRegistrationQueueDataHandler implements QueueDataHandler {

    private static final String DISCRIMINATOR_VALUE = "json-registration";

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private final Log log = LogFactory.getLog(JsonRegistrationQueueDataHandler.class);

    private Patient unsavedPatient;
    private String payload;
    Set<PersonAttribute> personAttributes;
    Location encounterLocation;
    private QueueProcessorException queueProcessorException;

    @Override
    public void process(final QueueData queueData) throws QueueProcessorException {
        log.info("Processing registration form data: " + queueData.getUuid());
        queueProcessorException = new QueueProcessorException();
        try {
            if (validate(queueData)) {
                registerUnsavedPatient();
                saveOtherPatientAttributesAsObs();
            }
        } catch (Exception e) {
            /*Custom exception thrown by the validate function should not be added again into @queueProcessorException.
             It should add the runtime dao Exception while saving the data into @queueProcessorException collection */
            if (!e.getClass().equals(QueueProcessorException.class)) {
                queueProcessorException.addException(e);
            }
        } finally {
            if (queueProcessorException.anyExceptions()) {
                throw queueProcessorException;
            }
        }
    }

    @Override
    public boolean validate(QueueData queueData) {
        log.info("Processing registration form data: " + queueData.getUuid());
        queueProcessorException = new QueueProcessorException();
        try {
            payload = queueData.getPayload();
            unsavedPatient = new Patient();
            populateUnsavedPatientFromPayload();
            validateUnsavedPatient();
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

    private void validateUnsavedPatient() {
        Patient savedPatient = findSimilarSavedPatient();
        if (savedPatient != null) {
            queueProcessorException.addException(new Exception("Found a patient with similar characteristic :  patientId = " + savedPatient.getPatientId()
                    + " Identifier Id = " + savedPatient.getPatientIdentifier().getIdentifier()));
        }
    }

    private void populateUnsavedPatientFromPayload() {
        setEncounterLocationFromPayload();
        setPatientIdentifiersFromPayload();
        setPatientBirthDateFromPayload();
        setPatientBirthDateEstimatedFromPayload();
        setPatientGenderFromPayload();
        setPatientNameFromPayload();
        setPatientAddressesFromPayload();
        setPatientAttibutesFromPayload();
        setDeceasedFromPayload();
    }

    private void setPatientIdentifiersFromPayload() {
        Set<PatientIdentifier> patientIdentifiers = new HashSet<PatientIdentifier>();
        PatientIdentifier preferredIdentifier = getPreferredPatientIdentifierFromPayload();
        if (preferredIdentifier != null) {
            patientIdentifiers.add(preferredIdentifier);
        }
        List<PatientIdentifier> otherIdentifiers = getOtherPatientIdentifiersFromPayload();
        if (!otherIdentifiers.isEmpty()) {
            patientIdentifiers.addAll(otherIdentifiers);
        }
        setIdentifierLocation(patientIdentifiers);
        unsavedPatient.setIdentifiers(patientIdentifiers);
    }

    private PatientIdentifier getPreferredPatientIdentifierFromPayload(){
        String identifierValue = JsonUtils.readAsString(payload, "$['patient']['patient.medical_record_number']");
        String identifierTypeName = "OpenMRS ID";

        PatientIdentifier preferredPatientIdentifier = createPatientIdentifier(identifierTypeName, identifierValue);
        if (preferredPatientIdentifier != null) {
            preferredPatientIdentifier.setPreferred(true);
            return preferredPatientIdentifier;
        } else {
            return null;
        }
    }

    private List<PatientIdentifier> getOtherPatientIdentifiersFromPayload() {
        List<PatientIdentifier> otherIdentifiers = new ArrayList<PatientIdentifier>();
        Object identifierTypeNameObject = JsonUtils.readAsObject(payload,"$['observation']['other_identifier_type']");
        Object identifierValueObject =JsonUtils.readAsObject(payload,"$['observation']['other_identifier_value']");

        if (identifierTypeNameObject instanceof JSONArray) {
            JSONArray identifierTypeName = (JSONArray) identifierTypeNameObject;
            JSONArray identifierValue = (JSONArray) identifierValueObject;
            for (int i = 0; i < identifierTypeName.size(); i++) {
                PatientIdentifier identifier = createPatientIdentifier(identifierTypeName.get(i).toString(), identifierValue.get(i).toString());
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
        PatientIdentifierType identifierType = Context.getPatientService().getPatientIdentifierTypeByName(identifierTypeName);
        if (identifierType == null) {
            queueProcessorException.addException(new Exception("Unable to find identifier type with name: " + identifierTypeName));
        } else if (identifierValue == null) {
            queueProcessorException.addException(new Exception("Identifier value can't be null type: " + identifierTypeName));
        } else {
            PatientIdentifier patientIdentifier = new PatientIdentifier();
            patientIdentifier.setIdentifierType(identifierType);
            patientIdentifier.setIdentifier(identifierValue);
            return patientIdentifier;
        }
        return null;
    }

    private void setIdentifierLocation(final Set<PatientIdentifier> patientIdentifiers) {
        Iterator<PatientIdentifier> iterator = patientIdentifiers.iterator();
        while (iterator.hasNext()) {
            PatientIdentifier identifier = iterator.next();
            identifier.setLocation(encounterLocation);
        }
    }

    private void setEncounterLocationFromPayload(){
        String locationIdString = JsonUtils.readAsString(payload, "$['encounter']['encounter.location_id']");
        if(locationIdString != null){
            int locationId = Integer.parseInt(locationIdString);
            encounterLocation = Context.getLocationService().getLocation(locationId);
            if (encounterLocation == null) {
                queueProcessorException.addException(new Exception("Unable to find encounter location using the id: " + locationIdString));
            }
        }
    }

    private void setPatientBirthDateFromPayload(){
        Date birthDate = JsonUtils.readAsDate(payload, "$['patient']['patient.birth_date']");
        unsavedPatient.setBirthdate(birthDate);
    }

    private void setPatientBirthDateEstimatedFromPayload(){
        boolean birthdateEstimated = JsonUtils.readAsBoolean(payload, "$['patient']['patient.birthdate_estimated']");
        unsavedPatient.setBirthdateEstimated(birthdateEstimated);
    }

    private void setPatientGenderFromPayload(){
        String gender = JsonUtils.readAsString(payload, "$['patient']['patient.sex']");
        unsavedPatient.setGender(gender);
    }

    private void setPatientNameFromPayload(){
        String givenName = JsonUtils.readAsString(payload, "$['patient']['patient.given_name']");
        String familyName = JsonUtils.readAsString(payload, "$['patient']['patient.family_name']");
        String middleName="";
        try{
            middleName= JsonUtils.readAsString(payload, "$['patient']['patient.middle_name']");
        } catch(Exception e){
            log.error(e);
        }

        PersonName personName = new PersonName();
        personName.setGivenName(givenName);
        personName.setMiddleName(middleName);
        personName.setFamilyName(familyName);
        unsavedPatient.addName(personName);
    }

    private void setDeceasedFromPayload(){
        boolean deceased = JsonUtils.readAsBoolean(payload, "$['patient']['patient.deceased']");
        if(deceased) {
            unsavedPatient.setDead(deceased);
            Date  dateOfDeath = JsonUtils.readAsDate(payload, "$['patient']['patient.date_of_death']");
            unsavedPatient.setDeathDate(dateOfDeath);
        }
    }

    private void setPatientAddressesFromPayload(){
        PersonAddress patientAddress = new PersonAddress();

        String postalAddress = JsonUtils.readAsString(payload, "$['patient']['patient.postal_address']");
        patientAddress.setAddress1(postalAddress);

        String landmark = JsonUtils.readAsString(payload, "$['patient']['patient.landmark']");
        patientAddress.setAddress2(landmark);

        String schoolOrEmployerAddress = JsonUtils.readAsString(payload, "$['patient']['patient.school_or_employer_address']");
        patientAddress.setAddress3(schoolOrEmployerAddress);

        String division = JsonUtils.readAsString(payload, "$['patient']['patient.division']");
        patientAddress.setAddress4(division);

        String subLocation = JsonUtils.readAsString(payload, "$['patient']['patient.sub_location']");
        patientAddress.setAddress5(subLocation);

        String location = JsonUtils.readAsString(payload, "$['patient']['patient.location']");
        patientAddress.setAddress6(location);

        String county = JsonUtils.readAsString(payload, "$['patient']['patient.county']");
        patientAddress.setCountry(county);

        String village = JsonUtils.readAsString(payload, "$['patient']['patient.village']");
        patientAddress.setCityVillage(village);

        String district = JsonUtils.readAsString(payload, "$['patient']['patient.district']");
        patientAddress.setCountyDistrict(district);

        String province = JsonUtils.readAsString(payload, "$['patient']['patient.province']");
        patientAddress.setStateProvince(province);

        String houseOrPlotNumber = JsonUtils.readAsString(payload, "$['patient']['patient.house_or_plot_number']");
        patientAddress.setPostalCode(houseOrPlotNumber);

        Set<PersonAddress> addresses = new TreeSet<PersonAddress>();
        addresses.add(patientAddress);
        unsavedPatient.setAddresses(addresses);
    }
    private void setPatientAttibutesFromPayload(){

        String telephone = JsonUtils.readAsString(payload, "$['patient']['patient.phone_number']");
        setAsAttribute(CommonMetadata._PersonAttributeType.TELEPHONE_CONTACT, telephone);

        String subChief = JsonUtils.readAsString(payload, "$['patient']['patient.subchief']");
        setAsAttribute(CommonMetadata._PersonAttributeType.SUBCHIEF_NAME, subChief);;

        String nextOfKinName = JsonUtils.readAsString(payload, "$['patient']['patient.next_of_kin_name']");
        setAsAttribute(CommonMetadata._PersonAttributeType.NEXT_OF_KIN_NAME, nextOfKinName);

        String nextOfKinRelationship =
                JsonUtils.readAsString(payload, "$['patient']['patient.next_of_kin_relationship']");
        setAsAttribute(CommonMetadata._PersonAttributeType.NEXT_OF_KIN_RELATIONSHIP, nextOfKinRelationship);

        String nextOfKinContact = JsonUtils.readAsString(payload, "$['patient']['patient.next_of_kin_contact']");
        setAsAttribute(CommonMetadata._PersonAttributeType.NEXT_OF_KIN_CONTACT, nextOfKinContact);

        String nextOfKinAddress = JsonUtils.readAsString(payload, "$['patient']['patient.next_of_kin_address']");
        setAsAttribute(CommonMetadata._PersonAttributeType.NEXT_OF_KIN_ADDRESS, nextOfKinAddress);
    }

    private void setAsAttribute(String attributeTypeUuid, String value){

        PersonService personService = Context.getPersonService();
        PersonAttributeType attributeType = personService.getPersonAttributeTypeByName(attributeTypeUuid);
        if(attributeType !=null && value != null){
            PersonAttribute personAttribute = new PersonAttribute(attributeType, value);
            personAttributes.add(personAttribute);
        } else if(attributeType ==null){
            queueProcessorException.addException(
                    new Exception("Unable to find Person Attribute type by uuid '" + attributeTypeUuid + "'")
            );
        }
    }

    private void saveOtherPatientAttributesAsObs(){

        String maritalStatus = JsonUtils.readAsString(payload, "$['patient']['patient.marital_status']");
        saveAsCodedObs(Dictionary.getConcept(Dictionary.CIVIL_STATUS), maritalStatus);

        String occupation = JsonUtils.readAsString(payload, "$['patient']['patient.occupation']");
        saveAsCodedObs(Dictionary.getConcept(Dictionary.OCCUPATION), occupation);

        String education = JsonUtils.readAsString(payload, "$['patient']['patient.education']");
        saveAsCodedObs(Dictionary.getConcept(Dictionary.EDUCATION), education);
    }

    private void saveAsCodedObs(Concept question, String value){
        if(StringUtils.isNotEmpty(value)) {
            String[] valueCodedElements = StringUtils.split(value, "\\^");
            int valueCodedId = Integer.parseInt(valueCodedElements[0]);
            Concept valueCoded = Context.getConceptService().getConcept(valueCodedId);
            if (valueCoded == null) {
                queueProcessorException.addException(new Exception("Unable to find concept for value coded with id: " + valueCodedId));
            } else {
                Obs obs = new Obs();
                obs.setPerson(unsavedPatient);
                obs.setConcept(question);
                obs.setObsDatetime(new Date());
                obs.setLocation(encounterLocation);
                obs.setValueCoded(valueCoded);
                Context.getObsService().saveObs(obs, "mUzima creating patient");
            }
        }
    }

    private void registerUnsavedPatient() {
        RegistrationDataService registrationDataService = Context.getService(RegistrationDataService.class);
        String temporaryUuid = getPatientUuidFromPayload();
        RegistrationData registrationData = registrationDataService.getRegistrationDataByTemporaryUuid(temporaryUuid);
        if (registrationData == null) {
            registrationData = new RegistrationData();
            registrationData.setTemporaryUuid(temporaryUuid);
            Context.getPatientService().savePatient(unsavedPatient);
            String assignedUuid = unsavedPatient.getUuid();
            registrationData.setAssignedUuid(assignedUuid);
            registrationDataService.saveRegistrationData(registrationData);
        }
    }

    private String getPatientUuidFromPayload(){
        return JsonUtils.readAsString(payload, "$['patient']['patient.uuid']");
    }

    private Patient findSimilarSavedPatient() {
        Patient savedPatient = null;
        if (unsavedPatient.getNames().isEmpty()) {
            PatientIdentifier identifier = unsavedPatient.getPatientIdentifier();
            if (identifier != null) {
                List<Patient> patients = Context.getPatientService().getPatients(identifier.getIdentifier());
                savedPatient = findPatient(patients, unsavedPatient);
            }
        } else {
            PersonName personName = unsavedPatient.getPersonName();
            List<Patient> patients = Context.getPatientService().getPatients(personName.getFullName());
            savedPatient = findPatient(patients, unsavedPatient);
        }
        return savedPatient;
    }

    private Patient findPatient(final List<Patient> patients, final Patient unsavedPatient) {
        for (Patient patient : patients) {
            // match it using the person name and gender, what about the dob?
            PersonName savedPersonName = patient.getPersonName();
            PersonName unsavedPersonName = unsavedPatient.getPersonName();
            if (StringUtils.isNotBlank(savedPersonName.getFullName())
                    && StringUtils.isNotBlank(unsavedPersonName.getFullName())) {
                if (StringUtils.equalsIgnoreCase(patient.getGender(), unsavedPatient.getGender())) {
                    if (patient.getBirthdate() != null && unsavedPatient.getBirthdate() != null
                            && DateUtils.isSameDay(patient.getBirthdate(), unsavedPatient.getBirthdate())) {
                        String savedGivenName = savedPersonName.getGivenName();
                        String unsavedGivenName = unsavedPersonName.getGivenName();
                        int givenNameEditDistance = StringUtils.getLevenshteinDistance(
                                StringUtils.lowerCase(savedGivenName),
                                StringUtils.lowerCase(unsavedGivenName));
                        String savedFamilyName = savedPersonName.getFamilyName();
                        String unsavedFamilyName = unsavedPersonName.getFamilyName();
                        int familyNameEditDistance = StringUtils.getLevenshteinDistance(
                                StringUtils.lowerCase(savedFamilyName),
                                StringUtils.lowerCase(unsavedFamilyName));
                        if (givenNameEditDistance < 3 && familyNameEditDistance < 3) {
                            return patient;
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public boolean accept(final QueueData queueData) {
        return StringUtils.equals(DISCRIMINATOR_VALUE, queueData.getDiscriminator());
    }
}
