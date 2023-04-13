package ext.renault.phenix.collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.ptc.arbortext.windchill.partlist.PartList;
import com.ptc.arbortext.windchill.partlist.PartToPartListLink;
import com.ptc.arbortext.windchill.translation.TranslationLink;
import com.ptc.core.meta.common.TypeIdentifier;

import ext.renault.phenix.utilities.SoftTypes;
import wt.configurablelink.ConfigurableLinkHelper;
import wt.configurablelink.ConfigurableReferenceLink;
import wt.enterprise.MadeFromLink;
import wt.enterprise.RevisionControlled;
import wt.epm.EPMDocument;
import wt.epm.modelitems.ModelItem;
import wt.epm.modelitems.ModelItemContainedIn;
import wt.epm.modelitems.link.ModelItemPartLink;
import wt.epm.structure.EPMDescribeLink;
import wt.epm.structure.EPMMemberLink;
import wt.epm.structure.EPMStructureHelper;
import wt.fc.Persistable;
import wt.fc.PersistenceHelper;
import wt.fc.QueryResult;
import wt.fc.ReferenceFactory;
import wt.fc.WTObject;
import wt.log4j.LogR;
import wt.part.WTPart;
import wt.part.WTPartMaster;
import wt.part.WTPartUsageLink;
import wt.pds.StatementSpec;
import wt.pom.PersistenceException;
import wt.query.QuerySpec;
import wt.query.SearchCondition;
import wt.util.WTAttributeNameIfc;
import wt.util.WTException;
import wt.vc.Mastered;
import wt.vc.VersionControlHelper;
import wt.vc.config.ConfigException;
import wt.vc.config.LatestConfigSpec;
import wt.vc.wip.WorkInProgressHelper;
import wt.vc.wip.Workable;

public class PhenixCollectorWorkers {

	public static enum BROWSABLE_LINKS {
		COLLECT_TOC("collectToc"), DOWNSTREAM_USAGE("collectPartUsage"), DESCRIBING("collectDescribing"), DOWNSTREAM_MEMBER("collectDocUsage"), UPSTREAM_USAGE(
				"collectUpstreamUsage"), DESCRIBED("collectDescribed"), UPSTREAM_MEMBER("collectUpstreamDocUsage"), TRANLATED("collectTranslated"), SOURCE(
				"collectSource"), CONFIGURABLEREFERENCELINK("collectConfigurableReferenceLink", true), PART_TO_PARTLIST("collectPartToPartList"),MODELITEM("collectModelItemLink");
		String method;
		boolean linkTypePossible;

		BROWSABLE_LINKS(String method) {
			this.method = method;
			this.linkTypePossible = false;
		}

		BROWSABLE_LINKS(String method, boolean linkTypeNeeded) {
			this.method = method;
			this.linkTypePossible = linkTypeNeeded;
		}
	};

	public static ArrayList<BROWSABLE_LINKS> DEFAULT_BROWSABLE_LINKS = new ArrayList<>(Arrays.asList(BROWSABLE_LINKS.DOWNSTREAM_USAGE,
			BROWSABLE_LINKS.DESCRIBING, BROWSABLE_LINKS.DOWNSTREAM_MEMBER));

	private static final Logger LOGGER = LogR.getLogger(PhenixCollectorWorkers.class.getName());
	//JIRA IRN59102-5160 Start
	static void collectToc(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws WTException {
		
		if (object instanceof WTPart) {

			boolean debugEnabled = LOGGER.isTraceEnabled();
			WTPart part = (WTPart) object;
			LOGGER.debug("Part Name =collectToc==>" + part.getName() + "  " + part.getNumber());
			if (debugEnabled) {
				LOGGER.trace("collectUsage from :" + part.getDisplayIdentity());
			}
			QueryResult childrens = PersistenceHelper.navigate(part, WTPartUsageLink.USES_ROLE, WTPartUsageLink.class, true);
			while (childrens.hasMoreElements() && !collector.isInterrupted) {

				WTPartMaster master = (WTPartMaster) childrens.nextElement();

				QueryResult df = VersionControlHelper.service.allIterationsOf(master);
				if (df.hasMoreElements()) {
					WTPart pa = (WTPart) df.nextElement();

					if (SoftTypes.isType(pa, SoftTypes.PUBLICATION_SECTION) || SoftTypes.isType(pa, SoftTypes.PUBLICATION_SECTION_RNO)
							|| SoftTypes.isType(pa, SoftTypes.PUBLICATION_SUMMARY)) {
						// Allowing only of above 3 types
						LOGGER.debug("Allowing part for collection ===>" + pa.getName() + "    " + pa.getNumber());
						collector.collectFrom(part, master, config, null, false, false);

					}

				}

			}
		}
	}
	//JIRA IRN59102-5160 End

	static void collectPartUsage(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws WTException {
		if (object instanceof WTPart) {

			boolean debugEnabled = LOGGER.isTraceEnabled();
			WTPart part = (WTPart) object;
			LOGGER.debug("Part Name =collectPartUsage==>" + part.getName() + "  " + part.getNumber());
			if (debugEnabled) {
				LOGGER.trace("collectUsage from :" + part.getDisplayIdentity());
			}
			QueryResult childrens = PersistenceHelper.navigate(part, WTPartUsageLink.USES_ROLE, WTPartUsageLink.class, true);
			while (childrens.hasMoreElements() &&  ! collector.isInterrupted) {
				WTPartMaster master = (WTPartMaster) childrens.nextElement();
				LOGGER.debug("master inside child Name =collectPartUsage==>" + master.getName() + "  " + master.getNumber());
				collector.collectFrom(part, master, config, null, false, false);
			}
		}
	}


	static void collectDescribing (PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws WTException {
		if (object instanceof WTPart) {
			boolean debugEnabled = LOGGER.isTraceEnabled();
			WTPart part = (WTPart) object;
			LOGGER.debug("Part Name =collectDescribing==>" + part.getName() + "  " + part.getNumber());
			if (debugEnabled) {
				LOGGER.trace("collectUsage from :" + part.getDisplayIdentity());
			}
			QueryResult childrens = PersistenceHelper.manager.navigate(part, EPMDescribeLink.DESCRIBED_BY_ROLE, EPMDescribeLink.class);

			while (childrens.hasMoreElements()) {
				EPMDocument doc = (EPMDocument) childrens.nextElement();
				if (VersionControlHelper.isLatestIteration(doc)) {
					if (WorkInProgressHelper.isCheckedOut((Workable) doc)) {
						if (WorkInProgressHelper.isWorkingCopy((Workable) doc)) {
							doc = (EPMDocument) WorkInProgressHelper.service.originalCopyOf((Workable) doc);
						}
					}
					LOGGER.debug("EPM DOc =collectDescribing==>" + doc.getName() + "  " + doc.getNumber());
					collector.collectFrom(part, doc, config, null, true, false);
					return;
				}
			}
		}
	}

	static void collectDescribed(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws PersistenceException,
			WTException {
		if (object instanceof EPMDocument) {
			EPMDocument doc = (EPMDocument) object;
			QueryResult parents = PersistenceHelper.manager.navigate(doc,EPMDescribeLink.DESCRIBES_ROLE, EPMDescribeLink.class);

			while (parents.hasMoreElements()) {
				WTPart latestParent = (WTPart) parents.nextElement();
				if (VersionControlHelper.isLatestIteration(latestParent)) {
					if (WorkInProgressHelper.isCheckedOut((Workable) latestParent)) {
						if (WorkInProgressHelper.isWorkingCopy((Workable) latestParent)) {
							latestParent = (WTPart) WorkInProgressHelper.service.originalCopyOf((Workable) latestParent);
						}
					}
					collector.collectFrom(doc, latestParent, config, null, false,false);
					return;
				}	
			}
		}
	}
	
	static void collectPartToPartList(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws ConfigException, WTException{
		if(object instanceof WTPart) {
			
				WTPart contentHolder = (WTPart)object;
				LOGGER.debug("DEBUG ==> collectPartToPartList  "+contentHolder.getName() );
				
				QueryResult sieIntqr = PersistenceHelper.manager.navigate(contentHolder,
						PartToPartListLink.DESCRIBED_BY_ROLE, PartToPartListLink.class);
				sieIntqr  = new LatestConfigSpec().process(sieIntqr);
				while (sieIntqr.hasMoreElements()) {
					PartList sieInt = (PartList)sieIntqr.nextElement();
					if (VersionControlHelper.isLatestIteration(sieInt)) {
						if (WorkInProgressHelper.isCheckedOut((Workable) sieInt)) {
							if (WorkInProgressHelper.isWorkingCopy((Workable) sieInt)) {
								sieInt = (PartList) WorkInProgressHelper.service.originalCopyOf((Workable) sieInt);
							}
						}
					collector.collectFrom(contentHolder,sieInt, config, null, false,false);
					return;
				}
				}
			}
		
	}

	static void collectDocUsage(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws ConfigException, WTException {
		if (object instanceof EPMDocument) {
			EPMDocument doc = (EPMDocument) object;
			QueryResult result = EPMStructureHelper.service.navigateUsesToIteration(doc, null, true, new LatestConfigSpec());
			while (result.hasMoreElements()) {
				Object child = result.nextElement();
				collector.collectFrom(doc, (WTObject) child, config, null, false,false);
			}
		}
	}

	static void collectUpstreamDocUsage(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws ConfigException, WTException {
		if (object instanceof EPMDocument) {
			EPMDocument doc = (EPMDocument) object;
			QueryResult parents = PersistenceHelper.navigate(doc.getMaster(), EPMMemberLink.USED_BY_ROLE, EPMMemberLink.class, true);
			QueryResult whereUses = new LatestConfigSpec().process(parents);
			while (whereUses.hasMoreElements() &&  ! collector.isInterrupted) {
				EPMDocument whereuse = (EPMDocument) whereUses.nextElement();
				if (whereuse.isLatestIteration())
				{
					collector.collectFrom(object, whereuse, config, null, true, false);
				}
			}
		}
	}

	static WTObject  CollectMadeFrom(WTObject object) throws WTException {
		QueryResult qrobjB = PersistenceHelper.manager.navigate(object, MadeFromLink.ROLE_BOBJECT_ROLE, MadeFromLink.class);
		while (qrobjB.hasMoreElements()) {
			WTObject objb= (WTObject) qrobjB.nextElement();

			return objb;
		}
		return null;
	}

	public static void collectTranslated(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws PersistenceException, WTException {
		QueryResult result = PersistenceHelper.manager.navigate(object, TranslationLink.ROLE_BOBJECT_ROLE, TranslationLink.class);

		while (result.hasMoreElements()) {
			WTObject translation = (WTObject) result.nextElement();
			collector.collectFrom(object, translation, config, null, false,false);
		}
	}

	public static void collectSource(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws PersistenceException, WTException {
		QueryResult result = PersistenceHelper.manager.navigate(object, TranslationLink.ROLE_AOBJECT_ROLE, TranslationLink.class);

		while (result.hasMoreElements()) {
			WTObject source = (WTObject) result.nextElement();
			collector.collectFrom(object, source, config, null, false,false);
		}
	}

	public static void collectUpstreamUsage(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws PersistenceException, WTException {
		if (object instanceof WTPart) {
			boolean debugEnabled = LOGGER.isTraceEnabled();
			WTPart part = (WTPart) object;
			if (debugEnabled) {
				LOGGER.trace("collectUsage from :" + part.getDisplayIdentity());
			}
			QueryResult parents = PersistenceHelper.navigate(part.getMaster(), WTPartUsageLink.USED_BY_ROLE, WTPartUsageLink.class, true);
			QueryResult whereUses = new LatestConfigSpec().process(parents);
			while (whereUses.hasMoreElements() &&  ! collector.isInterrupted) {
				WTPart whereuse = (WTPart) whereUses.nextElement();
				if (whereuse.isLatestIteration())
				{
					collector.collectFrom(object,whereuse, config, null, true,false);
				}
			}
		}
	}

	public static void collectConfigurableReferenceLink(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws WTException {
		QueryResult result = PersistenceHelper.manager.navigate(object, ConfigurableReferenceLink.ROLE_BOBJECT_ROLE, ConfigurableReferenceLink.class);
		while (result.hasMoreElements()) {
			WTObject child = (WTObject) result.nextElement();
			collector.collectFrom(object, child, config, null, false, false);
		}
		
		WTObject master = null;
		if(object instanceof RevisionControlled) {
			master = (WTObject) ((RevisionControlled) object).getMaster();
		}
		
		if (master != null) {
			result = PersistenceHelper.manager.navigate(master, ConfigurableReferenceLink.ROLE_AOBJECT_ROLE, ConfigurableReferenceLink.class);
			while (result.hasMoreElements()) {
				WTObject child = (WTObject) result.nextElement();
				collector.collectFrom(object, child, config, null, false, false);
			}
		}
	}

	public static void collectConfigurableReferenceLink(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config, TypeIdentifier linkType) throws WTException {
		ReferenceFactory refFactory = new ReferenceFactory();
		QueryResult result = ConfigurableLinkHelper.service.getOtherSideObjectsFromLink(refFactory.getReference(object), linkType, ConfigurableReferenceLink.ROLE_BOBJECT_ROLE, true);
		while(result.hasMoreElements()){
			WTObject child = (WTObject) result.nextElement();
			collector.collectFrom(object, child, config, null, false, false);
		}
		result = ConfigurableLinkHelper.service.getOtherSideObjectsFromLink(refFactory.getReference(object), linkType, ConfigurableReferenceLink.ROLE_AOBJECT_ROLE, true);
		while(result.hasMoreElements()){
			WTObject child = (WTObject) result.nextElement();
			collector.collectFrom(object, child, config, null, false, false);
		}
	}
	
	public static void collectModelItemLink(PhenixUserCollector collector, WTObject object, PhenixCollectorConfigSpec config) throws PersistenceException, WTException {
		if(object instanceof EPMDocument) {
			EPMDocument doc = (EPMDocument) object;
			QuerySpec modelItemQs = new QuerySpec();

			int idxModelItem = modelItemQs.appendClassList(ModelItem.class, false);
			int idxPart = modelItemQs.appendClassList(WTPart.class, true);
			int idxModelItemContainedIn = modelItemQs.appendClassList(ModelItemContainedIn.class, false);
			int idxModelItemPartLink = modelItemQs.appendClassList(ModelItemPartLink.class, false);

			SearchCondition modelItemContainedInSieSc = new SearchCondition(ModelItemContainedIn.class, ModelItemContainedIn.ROLE_AOBJECT_REF + "."
					+ WTAttributeNameIfc.REF_OBJECT_ID, SearchCondition.EQUAL, doc.getPersistInfo().getObjectIdentifier().getId());
			SearchCondition modelItemContainedModelItemSc = new SearchCondition(ModelItemContainedIn.class, ModelItemContainedIn.ROLE_BOBJECT_REF + "."
					+ WTAttributeNameIfc.REF_OBJECT_ID, ModelItem.class, WTAttributeNameIfc.ID_NAME);
			SearchCondition modelItemModelItemPartLinkSc = new SearchCondition(ModelItem.class, WTAttributeNameIfc.ID_NAME, ModelItemPartLink.class,
					ModelItemPartLink.ROLE_AOBJECT_REF + "." + WTAttributeNameIfc.REF_OBJECT_ID);
			SearchCondition modelItemPartLinkGieSc = new SearchCondition(ModelItemPartLink.class, ModelItemPartLink.ROLE_BOBJECT_REF + "."
					+ WTAttributeNameIfc.REF_OBJECT_ID, WTPart.class, WTPart.MASTER_REFERENCE + "." + WTAttributeNameIfc.REF_OBJECT_ID);
			SearchCondition latestGieSc = new SearchCondition(WTPart.class, WTPart.LATEST_ITERATION, SearchCondition.IS_TRUE);

			modelItemQs.appendWhere(modelItemContainedInSieSc, new int[] { idxModelItemContainedIn });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(modelItemContainedModelItemSc, new int[] { idxModelItemContainedIn, idxModelItem });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(modelItemModelItemPartLinkSc, new int[] { idxModelItem, idxModelItemPartLink });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(modelItemPartLinkGieSc, new int[] { idxModelItemPartLink, idxPart });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(latestGieSc, new int[] { idxPart });

			QueryResult aModelItemQR = PersistenceHelper.manager.find((StatementSpec) modelItemQs);
			while (aModelItemQR.hasMoreElements()) {
				Persistable[] persistables = (Persistable[]) aModelItemQR.nextElement();
				WTPart aPart = (WTPart) persistables[modelItemQs.getResultIndex(idxPart)];

				collector.collectFrom(object, aPart, config, null, false, false);
			}
		} else if (object instanceof WTPart) {
			WTPart part = (WTPart) object;
			QuerySpec modelItemQs = new QuerySpec();

			int idxModelItem = modelItemQs.appendClassList(ModelItem.class, false);
			int idxEPMDoc = modelItemQs.appendClassList(EPMDocument.class, true);
			int idxModelItemContainedIn = modelItemQs.appendClassList(ModelItemContainedIn.class, false);
			int idxModelItemPartLink = modelItemQs.appendClassList(ModelItemPartLink.class, false);

			
			SearchCondition modelItemPartLinkGieSc = new SearchCondition(ModelItemPartLink.class, ModelItemPartLink.ROLE_BOBJECT_REF + "."
					+ WTAttributeNameIfc.REF_OBJECT_ID, SearchCondition.EQUAL, part.getMaster().getPersistInfo().getObjectIdentifier().getId());
			
			SearchCondition modelItemModelItemPartLinkSc = new SearchCondition(ModelItem.class, WTAttributeNameIfc.ID_NAME, ModelItemPartLink.class,
					ModelItemPartLink.ROLE_AOBJECT_REF + "." + WTAttributeNameIfc.REF_OBJECT_ID);
			
			SearchCondition modelItemContainedModelItemSc = new SearchCondition(ModelItemContainedIn.class, ModelItemContainedIn.ROLE_BOBJECT_REF + "."
					+ WTAttributeNameIfc.REF_OBJECT_ID, ModelItem.class, WTAttributeNameIfc.ID_NAME);
			
			SearchCondition modelItemContainedInSieSc = new SearchCondition(ModelItemContainedIn.class, ModelItemContainedIn.ROLE_AOBJECT_REF + "."
					+ WTAttributeNameIfc.REF_OBJECT_ID, EPMDocument.class, WTAttributeNameIfc.ID_NAME);
			
			SearchCondition latestEpmSc = new SearchCondition(EPMDocument.class, EPMDocument.LATEST_ITERATION, SearchCondition.IS_TRUE);
			

			modelItemQs.appendWhere(modelItemPartLinkGieSc, new int[] { idxModelItemPartLink });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(modelItemModelItemPartLinkSc, new int[] { idxModelItem, idxModelItemPartLink });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(modelItemContainedModelItemSc, new int[] { idxModelItemContainedIn, idxModelItem });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(modelItemContainedInSieSc, new int[] { idxModelItemContainedIn, idxEPMDoc });
			modelItemQs.appendAnd();
			modelItemQs.appendWhere(latestEpmSc, new int[] { idxEPMDoc });
						
			QueryResult aModelItemQR = PersistenceHelper.manager.find((StatementSpec) modelItemQs);

			ArrayList<EPMDocument> epmList = new ArrayList<>();
			Map<Mastered, EPMDocument> masterList = new HashMap<>();
			while (aModelItemQR.hasMoreElements()) {
				Persistable[] persistables = (Persistable[]) aModelItemQR.nextElement();
				EPMDocument aDoc = (EPMDocument) persistables[modelItemQs.getResultIndex(idxEPMDoc)];
				
				if(masterList.containsKey(aDoc.getMaster())) {
					EPMDocument doc = masterList.get(aDoc.getMaster());
					if(aDoc.getVersionIdentifier().getVersionSortId().compareTo(doc.getVersionIdentifier().getVersionSortId()) > 0) {
						masterList.remove(doc.getMaster());
						epmList.remove(doc);
						epmList.add(aDoc);
						masterList.put(aDoc.getMaster(), aDoc);
					}
				} else {
					epmList.add(aDoc);
					masterList.put(aDoc.getMaster(), aDoc);
				}
				
			}
			
			for(EPMDocument aDoc : epmList) {
				collector.collectFrom(object, aDoc, config, null, false, false);
			}
		}
	}

}