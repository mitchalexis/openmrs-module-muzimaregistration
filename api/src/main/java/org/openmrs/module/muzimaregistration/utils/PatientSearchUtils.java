package org.openmrs.module.muzimaregistration.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.openmrs.Patient;
import org.openmrs.PersonName;
import org.openmrs.api.context.Context;
import org.openmrs.module.muzimaregistration.api.RegistrationDataService;
import org.openmrs.module.muzimaregistration.api.model.RegistrationData;

import java.util.List;

/**
 */
public class PatientSearchUtils {
    private PatientSearchUtils(){}
    public static Patient findPatient(final List<Patient> patients, final Patient unsavedPatient) {
        for (Patient patient : patients) {
            // match it using the person name and gender.
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

    public static Patient findSavedPatient(Patient candidatePatient, boolean searchRegistrationData){
        Patient savedPatient = null;
        if (StringUtils.isNotEmpty(candidatePatient.getUuid())) {
            savedPatient = Context.getPatientService().getPatientByUuid(candidatePatient.getUuid());
            if (savedPatient == null && searchRegistrationData == true) {
                String temporaryUuid = candidatePatient.getUuid();
                RegistrationDataService dataService = Context.getService(RegistrationDataService.class);
                RegistrationData registrationData = dataService.getRegistrationDataByTemporaryUuid(temporaryUuid);
                if(registrationData!=null) {
                    savedPatient = Context.getPatientService().getPatientByUuid(registrationData.getAssignedUuid());
                }
            }
        }
        if (savedPatient == null && !(candidatePatient.getPatientIdentifier() != null
                && StringUtils.isNotEmpty(candidatePatient.getPatientIdentifier().getIdentifier()) )) {
            List<Patient> patients = Context.getPatientService()
                    .getPatients(candidatePatient.getPatientIdentifier().getIdentifier());
            savedPatient = PatientSearchUtils.findPatient(patients, candidatePatient);
        }
        if(savedPatient == null && candidatePatient.getPersonName() != null
                && StringUtils.isNotEmpty(candidatePatient.getPersonName().getFullName())){
            List<Patient> patients = Context.getPatientService()
                    .getPatients(candidatePatient.getPersonName().getFullName());
            savedPatient = PatientSearchUtils.findPatient(patients, candidatePatient);
        }

        return savedPatient;
    }
}
