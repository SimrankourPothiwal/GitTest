package com.ibm.saas.accel.mw.pub.service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import javax.xml.bind.JAXBElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.saas.accel.mw.billing.service.ConnectionUtil;
import com.ibm.saas.accel.mw.billing.service.SoapRunService;
import com.ibm.saas.accel.mw.idm.bean.ControlExtensionProperty;
import com.ibm.saas.accel.mw.idm.bean.IDMCompositePartyBObjType;
import com.ibm.saas.accel.mw.idm.bean.ObjectFactory;
import com.ibm.saas.accel.mw.idm.bean.TCRMAdminContEquivBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMPartyAddressBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMPartyAddressPrivPrefBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMPartyContactMethodBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMPartyContactMethodPrivPrefBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMPartyIdentificationBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMPersonBObjType;
import com.ibm.saas.accel.mw.idm.bean.TCRMService;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.SaveResult;
import com.sforce.soap.enterprise.sobject.Contact;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.ConnectionException;

public class SfIdmPublishContactService implements SfIdmPublishContactConstants {

	static final Logger logger = LoggerFactory.getLogger(SfIdmPublishContactService.class);
	
	static ObjectFactory objFactory = new ObjectFactory();

	private Properties envProperties = new Properties();
	private Properties serviceProperties = new Properties();

	public Properties getEnvProperties() {
		return envProperties;
	}
	
	private static SfIdmPublishContactService instance;
	
	public static synchronized SfIdmPublishContactService getInstance() throws FileNotFoundException, IOException, ConnectionException {
		if (instance == null) {
			instance = new SfIdmPublishContactService();
		}
		return instance;
	}
	
	private EnterpriseConnection enterpriseConnection;	
	
	public Properties getServiceProperties() {
		return serviceProperties;
	}

	public SfIdmPublishContactService() throws FileNotFoundException, IOException, ConnectionException {
		super();

		try (FileInputStream in = new FileInputStream(ENV_PROPERTIES_FILE_PATH)) {
			envProperties.load(in);
		}
		
		try (InputStream in = SfIdmPublishContactService.class.getResourceAsStream(SERVICE_PROPERTIES_FILE_NAME)) { 
			serviceProperties.load(in);
		}
		
		enterpriseConnection = ConnectionUtil.createRenewableConnection(SECRET_FILE_PATH);
	}
	
	/**
	 * @param ceProperties
	 * @return
	 */
	public String getIDMEventType(List<ControlExtensionProperty> ceProperties){
		String eventType = null;
		
		if(ceProperties != null){
			for(ControlExtensionProperty ceProperty : ceProperties){
				if("IDMEventType".equalsIgnoreCase(ceProperty.getName())){
					eventType = ceProperty.getValue();
					break;
				}
			}
		}
		
		return eventType;
	}
	
	/**
	 * @param tcrmService
	 * @return
	 */
	public List<Contact> getIdsForUpdate(TCRMService tcrmService){
		JAXBElement<IDMCompositePartyBObjType> idmComposite = null;		
		TCRMPersonBObjType personObjType = null;
		List<Contact> contacts = new ArrayList<Contact>();

		String personId = null;
		String eventType = null;
		String emailSP = null;
		String phoneSP= null;
		String mailSP = null;
		String mobileSP = null;
		boolean isSuccessResponse = false;
		
		isSuccessResponse = RESULT_SUCCESS.equalsIgnoreCase(tcrmService.getResponseControl().getResultCode());
		eventType = getIDMEventType(tcrmService.getResponseControl().getDWLControl().getControlExtensionProperty());
		
		logger.debug("Request id: " + tcrmService.getResponseControl().getDWLControl().getRequestID());
		
		
		try {
			JAXBElement<IDMCompositePartyBObjType> jaxbElement = (JAXBElement<IDMCompositePartyBObjType>)tcrmService.getTxResponse().getResponseObject().getCommonBObj().get(0);
			idmComposite = jaxbElement;		
			
		} catch (NullPointerException | IndexOutOfBoundsException e) {
			logger.debug("Null values in response. " + e);
		}

		try {
			personObjType = idmComposite.getValue().getTCRMPersonBObj().get(0);
			personId = getActiveEID(personObjType);
			emailSP = getEmail_SP(personObjType);
			phoneSP = getPhoneSP(personObjType);
			mailSP = getMailSP(personObjType);
			mobileSP = getMobileSP(personObjType);
			logger.info("mobileSP " +mobileSP);

		
			
			if(personId == null){
				logger.info("Identification value is null");
				throw new NullPointerException("No EID for PersonObj");
			}
			
			
			if (eventType != null) {
				switch (eventType) {
				case IDM_EVENT_TYPE_PARTY_ADDED:
					logger.info("[PartyAdded]");
					contacts = processPartyAdded(personObjType.getTCRMAdminContEquivBObj(), personId, emailSP, phoneSP, mailSP, mobileSP);

					break;
				case IDM_EVENT_TYPE_PARTY_UPDATED:
					logger.info("[PartyUpdated]");
					contacts = processPartyAdded(personObjType.getTCRMAdminContEquivBObj(), personId, emailSP, phoneSP, mailSP, mobileSP);

					break;
				case IDM_EVENT_TYPE_PARTY_MERGED:
					logger.info("[PartyMerged]");
					contacts = processPartyAdded(personObjType.getTCRMAdminContEquivBObj(), personId, emailSP, phoneSP, mailSP, mobileSP);

					break;
				case IDM_EVENT_TYPE_PARTY_COLLAPSED:
					logger.info("[PartyCollapsed]");
					contacts = processPartyAdded(personObjType.getTCRMAdminContEquivBObj(), personId, emailSP, phoneSP, mailSP, mobileSP);

					break;

				default:
					break;
				}
			}
			
		} catch (NullPointerException | IndexOutOfBoundsException e) {
			System.out.println(e);
		}

		return contacts;
	}
	
	private String getMobileSP(TCRMPersonBObjType personObjType){
		String mobileSP = null;
		List<TCRMPartyContactMethodBObjType> tcrmPartyContactMethodBObjs = personObjType.getTCRMPartyContactMethodBObj();
		for (TCRMPartyContactMethodBObjType tcrmPartyContactMethodBObj : tcrmPartyContactMethodBObjs) {
			List<TCRMPartyContactMethodPrivPrefBObjType> tcrmPartyContactMethodPrivPrefBObj = tcrmPartyContactMethodBObj.getTCRMPartyContactMethodPrivPrefBObj();
			if(tcrmPartyContactMethodPrivPrefBObj.size() > 0){
			if("CEL".equals(tcrmPartyContactMethodPrivPrefBObj.get(0).getPrivPrefValue())){ // Privpref for mobile
				mobileSP =  tcrmPartyContactMethodPrivPrefBObj.get(0).getValueString().toUpperCase();
				return mobileSP;
		}else{
			mobileSP = null;
		}
		}
		}
		return mobileSP;
	}
	
	
	private String getMailSP(TCRMPersonBObjType personObjType){
		String mailSP = null;
		List<TCRMPartyAddressBObjType> tcrmPartyAddressBObjs = personObjType.getTCRMPartyAddressBObj();
		for (TCRMPartyAddressBObjType tcrmPartyAddressBObj : tcrmPartyAddressBObjs) {
			List<TCRMPartyAddressPrivPrefBObjType> tcrmPartyAddressPrivPrefBObjs = tcrmPartyAddressBObj.getTCRMPartyAddressPrivPrefBObj();
			if(tcrmPartyAddressBObjs.size() > 0){
				if("MAI".equals(tcrmPartyAddressPrivPrefBObjs.get(0).getPrivPrefValue())){
					mailSP = tcrmPartyAddressPrivPrefBObjs.get(0).getValueString().toUpperCase();
					return mailSP;
				}
				else{
					mailSP = null;
				}
			}
		}
		return mailSP;
	}
	
	private String getPhoneSP(TCRMPersonBObjType personObjType){
		String phoneSP = null;
		List<TCRMPartyContactMethodBObjType> tcrmPartyContactMethodBObjs = personObjType.getTCRMPartyContactMethodBObj();
		for (TCRMPartyContactMethodBObjType tcrmPartyContactMethodBObj : tcrmPartyContactMethodBObjs) {
			List<TCRMPartyContactMethodPrivPrefBObjType> tcrmPartyContactMethodPrivPrefBObj = tcrmPartyContactMethodBObj.getTCRMPartyContactMethodPrivPrefBObj();
			if(tcrmPartyContactMethodPrivPrefBObj.size() > 0){
			if("TEL".equals(tcrmPartyContactMethodPrivPrefBObj.get(0).getPrivPrefValue())){ // Privpref for phone
				phoneSP =  tcrmPartyContactMethodPrivPrefBObj.get(0).getValueString().toUpperCase();
				return phoneSP;
		}else{
			phoneSP = null;
		}
		}
		}
		return phoneSP;
	}
		
	
	
	/**
	 * @param personObjType
	 * @return
	 */
	public String getActiveEID(TCRMPersonBObjType personObjType){
		String personId = null;
		List<TCRMPartyIdentificationBObjType> idents = personObjType.getTCRMPartyIdentificationBObj();
		
		for(TCRMPartyIdentificationBObjType ident: idents){
			if("EID".equals(ident.getIdentificationValue()) && ident.getEndDate() == null){ // Active EID
				personId = ident.getIdentificationNumber();
			}
		}
		
		return personId;
	}
	
	/**
	 * @param personObjType
	 * @return emailSP
	 */
	public String getEmail_SP(TCRMPersonBObjType personObjType) {
		String emailSP=null;
	
		List<TCRMPartyContactMethodBObjType> tcrmPartyContactMethodBObjs = personObjType.getTCRMPartyContactMethodBObj();
		for(TCRMPartyContactMethodBObjType tcrmPartyContactMethodBObj: tcrmPartyContactMethodBObjs){
			List<TCRMPartyContactMethodPrivPrefBObjType> tcrmPartyContactMethodPrivPrefBObjType = tcrmPartyContactMethodBObj.getTCRMPartyContactMethodPrivPrefBObj();
			if(tcrmPartyContactMethodPrivPrefBObjType.size() > 0){
			if("EML".equals(tcrmPartyContactMethodPrivPrefBObjType.get(0).getPrivPrefValue()) && tcrmPartyContactMethodPrivPrefBObjType.get(0).getValueString() != null){ // Privpref for email
				emailSP =  tcrmPartyContactMethodPrivPrefBObjType.get(0).getValueString();
				return emailSP;
		}else{
			emailSP = null;
		}
		}
		}
		return emailSP;
	}

public String[] getSP(TCRMPersonBObjType personObjType) {
		// String emailSP=null;
        // String phoneSP = null;
        String[] arr = new String[3];
	
		List<TCRMPartyContactMethodBObjType> tcrmPartyContactMethodBObjs = personObjType.getTCRMPartyContactMethodBObj();
		for(TCRMPartyContactMethodBObjType tcrmPartyContactMethodBObj: tcrmPartyContactMethodBObjs){
			List<TCRMPartyContactMethodPrivPrefBObjType> tcrmPartyContactMethodPrivPrefBObjType = tcrmPartyContactMethodBObj.getTCRMPartyContactMethodPrivPrefBObj();
			if(tcrmPartyContactMethodPrivPrefBObjType.size() > 0){
			if("EML".equals(tcrmPartyContactMethodPrivPrefBObjType.get(0).getPrivPrefValue()) && tcrmPartyContactMethodPrivPrefBObjType.get(0).getValueString() != null){ // Privpref for email
				arr[0] =  tcrmPartyContactMethodPrivPrefBObjType.get(0).getValueString();
				
		    }
            if("TEL".equals(tcrmPartyContactMethodPrivPrefBObjType.get(0).getPrivPrefValue()){
                arr[1] =  tcrmPartyContactMethodPrivPrefBObjType.get(0).getValueString();
            }
            if("CEL".equals(tcrmPartyContactMethodPrivPrefBObjType.get(0).getPrivPrefValue()){
                arr[2] =  tcrmPartyContactMethodPrivPrefBObjType.get(0).getValueString();
            }
		
        }
		}
		return arr;
	}



    

	/**
	 * @param conts
	 * @param personId
	 * @return
	 */
	public List<Contact> processPartyAdded(List<TCRMAdminContEquivBObjType> conts, String personId, String emailSP, String phoneSP, String mailSP, String mobileSP) {

		List<Contact> contacts = new ArrayList<Contact>();
		for(TCRMAdminContEquivBObjType cont: conts){
			String sfdcId = cont.getAdminPartyId();
			
			if(ADMIN_SYSTEM_VALUE.equals(cont.getAdminSystemValue()) &&	sfdcId != null && sfdcId.startsWith("003")){ // search for SFDC key and Contact
				Contact contact = new Contact();
			
				contact.setId(sfdcId);
//				contact.setIDM_ID__c(personId);
//				contact.setIDM_Response_DateTime__c(Calendar.getInstance());
//				contact.setIDM_Response_Status__c(CONTACT_STATUS_COMPLETE);
//				contact.setFieldsToNull(new String[]{"IDM_Error_Message__c"});

				contact.setExternal_IDMid__c(personId);
				
				contact.setEmail_P_S__c(emailSP);
				contact.setPhone_P_S__c(phoneSP);
				
				contact.setMailing_Address_P_S__c(mailSP);
				contact.setMobile_P_S__c(mobileSP);
				
				contacts.add(contact);
				logger.info("SF(AdminPartyId)/Person id(Identification Number)/Email_P_S__c/Mailing_Address_P_S__c/Phone_P_S__c/Mobile_P_S__c: [" + sfdcId + "/" + personId  + "/" + emailSP + "/" + mailSP  + "/"  + phoneSP + "/" + mobileSP +"]");
			}
		}
		return contacts;
	}  
	
	

	/**
	 * @param connection
	 * @param contacts
	 * @throws ConnectionException
	 */
	public synchronized void update(List<Contact> contacts) throws ConnectionException{
		
		if (contacts.size() > 0) {

			List<SaveResult> sr = null;

			logger.debug("Config SessionId: " + enterpriseConnection.getConfig().getSessionId().substring(enterpriseConnection.getSessionHeader().getSessionId().length() - 20, enterpriseConnection.getSessionHeader().getSessionId().length()));
			logger.debug("Endpoint: " + enterpriseConnection.getConfig().getServiceEndpoint());
			
			//sr = enterpriseConnection.update(sObjects);
			sr = SoapRunService.update(enterpriseConnection, contacts);
			//logger.info("sr :" +sr);
			
			// update SessionHeader with sessionId in case it was renewed
			enterpriseConnection.getSessionHeader().setSessionId(enterpriseConnection.getConfig().getSessionId());
			
			for(int i=0; i<sr.size(); i++){
				boolean isSuccess = sr.get(i).getSuccess();
				
				logger.debug("Update status for Id: " + contacts.get(i).getId() + " - Success: " + isSuccess);
				

				if (!isSuccess) {
					// Error
					String errMsg = sr.get(i).getErrors()[0].getMessage();
					String errDescription = null;
					String idmId = contacts.get(i).getIDM_ID__c();
					logger.info("IDM_ID__c: "+ idmId);
					
					if(errMsg != null & errMsg.contains("invalid cross reference id")){
						logger.debug("Update error for Contact Id: [" + contacts.get(i).getId() + "]. Description: " + errMsg);
					} else {
						logger.error("Update error for Contact Id: [" + contacts.get(i).getId() + "]. Description: " + errMsg);
					}
					
					if(!errMsg.contains("IDM_ID__c duplicates")){
						// write error description
						errDescription = idmId + " - (IDM ID): " + errMsg;
					} else { 
						// Duplicate error - IDM_ID__c
						errDescription = "existing IDMid is no longer valid, please use the contact associated with IDMid " + idmId;
					}
					
					Contact contact = setContactError(contacts.get(i).getId(), errDescription);

					//SaveResult[] srErr = enterpriseConnection.update(errObjects);
					List<SObject> errContact = new ArrayList<SObject>();
					errContact.add(contact);
					List<SaveResult> srErr = SoapRunService.update(enterpriseConnection, errContact);
					// update SessionHeader with sessionId in case it was renewed
					enterpriseConnection.getSessionHeader().setSessionId(enterpriseConnection.getConfig().getSessionId());
					
					logger.debug("Error update status Success: " + srErr.get(0).getSuccess());
				}
				
			}

			logger.debug("SalesForce Object(s) update finished.");

			} else {
			String msg = "Nothing to update. CIPNA";
			logger.info(msg);
			//throw new RuntimeException(err);
		}

	}
	
	private Contact setContactError(String id, String error){
		logger.debug("Update error message: " + error);
		
		Contact contact = new Contact();
		
		contact.setId(id);
		contact.setIDM_Response_DateTime__c(Calendar.getInstance());
		contact.setIDM_Response_Status__c(CONTACT_STATUS_ERROR);
		contact.setIDM_Error_Message__c(error);

		return contact;
	}
	
	
}
