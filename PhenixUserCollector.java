package ext.renault.phenix.collector;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.ptc.arbortext.windchill.partlist.PartList;
import com.ptc.core.lwc.server.TypeDefinitionServiceHelper;
import com.ptc.core.meta.common.TypeIdentifier;
import com.ptc.core.meta.common.TypeIdentifierHelper;
import com.ptc.mvc.components.ComponentParams;

import ext.renault.phenix.collector.PhenixCollectorWorkers.BROWSABLE_LINKS;
import ext.renault.phenix.collector.PhenixProduction.PRODCOLLECTIONS;
import ext.renault.phenix.expressions.service.AdvancedExpressionHelper;
import ext.renault.phenix.resource.PhenixCollectorResource;
import wt.enterprise.RevisionControlled;
import wt.epm.EPMDocument;
import wt.fc.ObjectReference;
import wt.fc.Persistable;
import wt.fc.PersistenceHelper;
import wt.fc.QueryResult;
import wt.fc.WTObject;
import wt.fc.collections.WTArrayList;
import wt.fc.collections.WTHashSet;
import wt.fc.collections.WTKeyedHashMap;
import wt.fc.collections.WTList;
import wt.fc.collections.WTSet;
import wt.fc.collections.WTValuedMap;
import wt.fc.collections.WTValuedMap.WTValuedEntry;
import wt.inf.container.OrgContainer;
import wt.inf.container.WTContainerHelper;
import wt.log4j.LogR;
import wt.org.WTUser;
import wt.part.WTPart;
import wt.pds.StatementSpec;
import wt.pom.PersistenceException;
import wt.preference.PreferenceClient;
import wt.preference.PreferenceHelper;
import wt.query.ClassAttribute;
import wt.query.OrderBy;
import wt.query.QuerySpec;
import wt.query.SearchCondition;
import wt.session.SessionHelper;
import wt.util.WTAttributeNameIfc;
import wt.util.WTException;
import wt.util.WTMessage;
import wt.util.WTPropertyVetoException;
import wt.vc.Iterated;
import wt.vc.Mastered;
import wt.vc.VersionControlHelper;
import wt.vc.Versioned;
import wt.vc.config.ConfigHelper;
import wt.vc.config.LatestConfigSpec;
import wt.vc.wip.WorkInProgressHelper;
import wt.vc.wip.Workable;

public class PhenixUserCollector extends PhenixCollector {

	public enum USERSCOLLECTIONS {
		INIT, COLLECTED_TOC, COLLECTED, MASTER, MASTER_LINK, CONF_NODES, NEUTRAL_NODES, END_NODES, DISCRIBING_END_NODES, ALL, MODIFIED, ADDED, REVISED, MADEFROM, CHANGE_REF, FILTERED, FILTEREDBYLINK, REMOVEDS, REMOVED_BY_USER, TRANSVERSE_OBJECTS
	};

	public enum USERSCOLLECTIONSRECOLLECT {
		R_ALL, R_ALL_APPLI, R_FILTERED, R_TCH, R_SIES, R_SATELLITES_OBJECTS
	};

	public boolean recollect;
	public boolean inited;
	public boolean refreshed;
	public Timestamp lastrefresh;
	public Timestamp lastuse;
	public String username;
	public Set<String> appli;
	public boolean tofilter;
	public String number;
	public String showmode;
	public boolean docdriven;
	public boolean propagate;
	public boolean displayChanged;
	public boolean isInterrupted;
	public boolean appliand;
	public boolean autorefresh;
	public boolean refreshing;
	public boolean extended;
	public PhenixCollectorConfigSpec defautConfigSpec;
	public Set<PhenixCollectorConfigSpec> configSpecs;
	public String currentConfigSpecName;
	public PhenixBrowsableLinkService browsableLinksService;
	public PhenixProduction production;
	public PhenixUserCollectorTracker tracker;
	public PhenixUserCollectorCarryOverTotalTracker carryOverTotalTracker;
	private Set<TypeIdentifier> uniqueTypes;
	WTKeyedHashMap modificationMap;
	WTSet needDelete;
	int count;
	public int gaugeMedian;

	private static final Logger LOGGER = LogR.getLogger(PhenixUserCollector.class.getName());
	final private static String RESOURCE = PhenixCollectorResource.class.getName();

	public PhenixUserCollector() throws WTException {
		super();
		this.username = SessionHelper.getPrincipal().getName();
		this.appli = new HashSet<>();
		this.number = null;
		this.docdriven = true;
		this.propagate = false;
		this.displayChanged = false;
		this.showmode = "showonly";
		this.isInterrupted = false;
		this.production = null;
		this.inited = false;
		this.extended = false;
		this.browsableLinksService = new PhenixBrowsableLinkService();
		this.uniqueTypes = new HashSet<>();
	}

	private void init() throws WTException {
		this.refreshed = false;
		Date date = new Date();
		this.autorefresh = false;
		this.refreshing = false;
		this.configSpecs = new HashSet<PhenixCollectorConfigSpec>();
		this.modificationMap = new WTKeyedHashMap();
		this.needDelete = new WTHashSet();
		this.recollect = false;
		this.tracker = null;
		this.carryOverTotalTracker = null;
		currentConfigSpecName = "Default";

		WTUser user = (WTUser) SessionHelper.manager.getPrincipal();
		OrgContainer orgContainer = WTContainerHelper.service.getOrgContainer(user.getOrganization());
		String configSpecsPref = (String) PreferenceHelper.service.getValue("PhenixCollectorConfigSpecs", PreferenceClient.WINDCHILL_CLIENT_NAME, orgContainer);

		if (configSpecsPref != null && !configSpecsPref.isEmpty() && !configSpecsPref.equals("Default")) {
			String[] configs = configSpecsPref.split("\\|");
			for (String config : configs) {
				PhenixCollectorConfigSpec cs = new PhenixCollectorConfigSpec(config);
				if (cs.name.equals(PhenixCollectorConfigSpec.DEFAULT_CONFIGSPEC_NAME)) {
					this.defautConfigSpec = cs;
				} else {
					this.configSpecs.add(cs);
				}
			}
		}
		if (this.defautConfigSpec == null) {
			this.defautConfigSpec = new PhenixCollectorConfigSpec();
		}

		String browsableLinksPref = (String) PreferenceHelper.service.getValue("PhenixCollectorBrowsableLinks", PreferenceClient.WINDCHILL_CLIENT_NAME, orgContainer);

		if (browsableLinksPref != null && !browsableLinksPref.isEmpty()) {
			String[] internalValues = browsableLinksPref.split(",");
			for (String internalValue : internalValues) {

				String value = internalValue;
				String displayValue = null;
				TypeIdentifier linkType = null;
				if (value.indexOf('/') != -1) {
					String[] values = value.split("/");
					value = values[0];
					linkType = TypeDefinitionServiceHelper.service.getTypeIdentifier(values[1]);
					if (linkType == null) {
						value = null;
					} else {
						displayValue = TypeDefinitionServiceHelper.service.getTypeDefView(linkType).getDisplayName();
					}
				}

				for (BROWSABLE_LINKS bl : PhenixCollectorWorkers.BROWSABLE_LINKS.values()) {
					if (bl.name().equals(value) && (linkType == null || bl.linkTypePossible)) {
						if (displayValue == null) {
							WTMessage m = new WTMessage(RESOURCE, value, null);
							displayValue = m.toString();
						}
						this.browsableLinksService.addBrowsableLink(new PhenixBrowsableLink(displayValue, internalValue, bl.method, linkType));
					}
				}
			}
		}

		initDefaultBrowsableLinks();

		Integer medianPref = (Integer) PreferenceHelper.service.getValue("PhenixCollectorGaugeMedian", PreferenceClient.WINDCHILL_CLIENT_NAME, orgContainer);
		if (medianPref == null || medianPref.intValue() == 0)
			this.gaugeMedian = 10000;
		else
			this.gaugeMedian = medianPref.intValue();

	}

	public void init(String number) throws WTPropertyVetoException, WTException {
		init();
		QuerySpec querySpec = new QuerySpec(WTPart.class);
		int[] idxWTPart = { 0 };
		querySpec.appendWhere(new SearchCondition(WTPart.class, WTPart.NUMBER, SearchCondition.LIKE, number), idxWTPart);

		querySpec = new LatestConfigSpec().appendSearchCriteria(querySpec);

		OrderBy ob2 = new OrderBy(new ClassAttribute(WTPart.class, WTAttributeNameIfc.ID_NAME), true);

		querySpec.appendOrderBy(ob2, idxWTPart);

		QueryResult qr = PersistenceHelper.manager.find((StatementSpec) querySpec);

		if (qr.size() > 0) {
			this.getCollection(USERSCOLLECTIONS.INIT).clear();
			this.getCollection(USERSCOLLECTIONS.INIT).add((WTObject) qr.nextElement());
		}
	}

	public void init(WTSet set) throws WTPropertyVetoException, WTException {
		init();
		this.getCollection(USERSCOLLECTIONS.INIT).clear();
		this.getCollection(USERSCOLLECTIONS.INIT).addAll(set);
		this.inited = true;
	}

	public void runDownStreamCollection() throws WTException, WTPropertyVetoException {
		this.count = 0;
		for (Object objectref : this.getCollection(USERSCOLLECTIONS.INIT).content) {

			Object object = ((ObjectReference) objectref).getObject();
			collectFrom(null, (WTObject) object, defautConfigSpec, null, false, false);
		}
		makeoutputcollections();

		LOGGER.info("Collector  return :" + this.getCollection(USERSCOLLECTIONS.ALL).content.size() + " Objects");

		if (this.tracker != null) {
			if (!this.tracker.initialized) {
				this.tracker.save();
			} else {
				this.tracker.makeDiff();
			}

		}

		Date date = new Date();
		this.lastrefresh = new Timestamp(date.getTime());

	}

	public void refresh() throws WTException, WTPropertyVetoException {
		this.recollect = false;
		this.refreshing = true;

		ArrayList<PhenixBrowsableLink> currentLinks = this.browsableLinksService.getCurrentBrowsableLinks();
		this.browsableLinksService.clearCurrentBrowsableLinks();

		initDefaultBrowsableLinks();

		/* refresh lastest */
		WTSet modified = this.getCollection(USERSCOLLECTIONS.COLLECTED).refresh();

		for (Object objectref : modified) {

			Object object = ((ObjectReference) objectref).getObject();

			if (object instanceof RevisionControlled) {
				RevisionControlled rc = (RevisionControlled) object;
				Iterated newversion = VersionControlHelper.service.getLatestIteration(rc, false);
				this.getCollection(USERSCOLLECTIONS.MODIFIED).add((WTObject) newversion);
				if (PhenixUserCollectorObjectsHelper.isFromSoftypeEnum((WTObject) newversion, PhenixUserCollectorObjectsHelper.CONFIGSPEC_OBJECTS.class)) {
					collectFrom(null, (WTObject) newversion.getMaster(), null, (WTObject) object, false, false);
				} else {
					collectFrom(null, (WTObject) newversion, null, (WTObject) object, false, false);
				}

			}
		}

		/* refresh revision on latest conf only */
		WTSet tocheck = new WTHashSet();
		WTSet revised = new WTHashSet();
		tocheck.addAll(this.getCollection(USERSCOLLECTIONS.END_NODES).content);
		tocheck.addAll(this.getCollection(USERSCOLLECTIONS.CONF_NODES).content);
		tocheck.addAll(this.getCollection(USERSCOLLECTIONS.NEUTRAL_NODES).content);

		WTValuedMap originalLatestVersionsMap = VersionControlHelper.service.getLatestRevisions(tocheck);
		for (Iterator<?> originalLatestEntries = originalLatestVersionsMap.entrySet().iterator(); originalLatestEntries.hasNext();) {
			WTValuedEntry originalLatestEntry = (WTValuedEntry) originalLatestEntries.next();
			Versioned originalVersion = (Versioned) originalLatestEntry.getKeyAsPersistable();
			Versioned latestVersion = (Versioned) originalLatestEntry.getValueAsPersistable();
			if (!originalVersion.equals(latestVersion) && !WorkInProgressHelper.isCheckedOut((Workable) originalVersion)) {

				this.getCollection(USERSCOLLECTIONS.REVISED).add((WTObject) latestVersion);
				this.removeObjectFromCollector((WTObject) originalVersion,  false);
				this.getCollection(USERSCOLLECTIONS.CHANGE_REF).add(this.getCollection(USERSCOLLECTIONS.REVISED), (WTObject) originalVersion, (WTObject) latestVersion, true, true);
				revised.add(originalVersion);
				if (PhenixUserCollectorObjectsHelper.isFromSoftypeEnum((WTObject) latestVersion, PhenixUserCollectorObjectsHelper.CONFIGSPEC_OBJECTS.class)) {
					collectFrom(null, (WTObject) latestVersion.getMaster(), null, (WTObject) originalVersion, false, false);
				} else {
					collectFrom(null, (WTObject) latestVersion, null, (WTObject) originalVersion, false, false);
				}

			}

		}

		if (this.production != null) {
			this.production.refresh();
		}
		makeoutputcollections();

		Date date = new Date();
		this.lastrefresh = new Timestamp(date.getTime());
		this.modificationMap.clear();
		this.refreshing = false;

		this.browsableLinksService.setCurrentBrowsableLinks(currentLinks);

	}

	public void makeoutputcollections() {
		String configSpecName = this.currentConfigSpecName;
		if (configSpecName.equals("Default")) {
			configSpecName = null;
		}
		purgeCollector();
		this.getCollection(USERSCOLLECTIONS.REMOVEDS).clear();
		this.getCollection(USERSCOLLECTIONS.ALL).clear();
		this.getCollection(USERSCOLLECTIONS.ALL).addAll(this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configSpecName));
		this.getCollection(USERSCOLLECTIONS.ALL).addAll(this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configSpecName));
		if (configSpecName == null) {
			this.getCollection(USERSCOLLECTIONS.ALL).addAll(this.getCollection(USERSCOLLECTIONS.CONF_NODES));
			this.getCollection(USERSCOLLECTIONS.ALL).addAll(this.getCollection(USERSCOLLECTIONS.NEUTRAL_NODES));
		}

		// activate debug on ext.renault.phenix.collector.PhenixCollection for
		// printing */
		this.getCollection(USERSCOLLECTIONS.COLLECTED).Print();
		this.getCollection(USERSCOLLECTIONS.MASTER).Print();
		this.getCollection(USERSCOLLECTIONS.MASTER_LINK).Print();
		this.getCollection(USERSCOLLECTIONS.CONF_NODES).Print();
		this.getCollection(USERSCOLLECTIONS.END_NODES).Print();
		this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).Print();
		this.getCollection(USERSCOLLECTIONS.NEUTRAL_NODES).Print();
		this.getCollection(PRODCOLLECTIONS.WORKNODES).Print();
		this.getCollection(PRODCOLLECTIONS.WORKITEMS).Print();
		this.getCollection(USERSCOLLECTIONS.MADEFROM).Print();

	}

	public void collectFrom(WTObject parent, WTObject entry, PhenixCollectorConfigSpec config, WTObject previous, boolean reverse, boolean recollect) throws PersistenceException, WTException {
		WTObject object = null;
		Mastered master = null;
		if (entry instanceof Mastered) {
			master = (Mastered) entry;
			QueryResult qr = ConfigHelper.service.filteredIterationsOf(master, this.defautConfigSpec.configSpecs);
			object = (WTObject) qr.nextElement();
			if (WorkInProgressHelper.isCheckedOut((Workable) object)) {
				if (WorkInProgressHelper.isWorkingCopy((Workable) object)) {
					object = (WTObject) WorkInProgressHelper.service.originalCopyOf((Workable) object);
				}
			}
			this.getCollection(USERSCOLLECTIONS.MASTER).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), (WTObject) master, object, true, true);

		}

		if (entry instanceof EPMDocument || entry instanceof WTPart || entry instanceof PartList) {
			object = entry;
		}

		processCollected(parent, object, config, previous, false, recollect);

		if (master != null && PhenixUserCollectorObjectsHelper.isFromSoftypeEnum(object, PhenixUserCollectorObjectsHelper.CONFIGSPEC_OBJECTS.class)) {
			Iterator<PhenixCollectorConfigSpec> it = this.configSpecs.iterator();
			while (it.hasNext()) {
				PhenixCollectorConfigSpec config1 = it.next();
				QueryResult qr = ConfigHelper.service.filteredIterationsOf(master, config1.configSpecs);
				if (qr.hasMoreElements()) {
					WTObject objectconfig = (WTObject) qr.nextElement();
					if (WorkInProgressHelper.isCheckedOut((Workable) objectconfig)) {
						if (WorkInProgressHelper.isWorkingCopy((Workable) objectconfig)) {
							objectconfig = (EPMDocument) WorkInProgressHelper.service.originalCopyOf((Workable) object);
						}
					}
					this.getCollection(USERSCOLLECTIONS.MASTER).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), (WTObject) master, objectconfig, true, true);
					processCollected(parent, objectconfig, config1, previous, reverse, false);
				}
			}
		}
	}

	private void processCollected(WTObject parent, WTObject object, PhenixCollectorConfigSpec config, WTObject previous, boolean reverse, boolean recollected) throws WTException {
		String configspecname = null;
		boolean alreadyCollected = false;

		if (this.getCollection(USERSCOLLECTIONS.COLLECTED).content.contains(object)) {
			alreadyCollected = true;
		} else {
			addType(object);
		}

		if (config != null && !config.name.equals(PhenixCollectorConfigSpec.DEFAULT_CONFIGSPEC_NAME)) {
			configspecname = config.name;
		}

		if (PhenixUserCollectorObjectsHelper.isFromSoftypeEnum(object, PhenixUserCollectorObjectsHelper.CONFNODE_OBJECTS.class)) {
			this.getCollection(USERSCOLLECTIONS.CONF_NODES).getCollection(configspecname).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), object, object, true, true);

		} else if (PhenixUserCollectorObjectsHelper.isFromSoftypeEnum(object, PhenixUserCollectorObjectsHelper.END_OBJECTS.class)) {
			this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configspecname).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), object, object, true, true);
			if (reverse) {
				this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configspecname).add(this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configspecname), object,
						parent, true, true);
			}

			if (alreadyCollected) {
				WTObject describing = this.getCollection(USERSCOLLECTIONS.END_NODES).getRelated(USERSCOLLECTIONS.DISCRIBING_END_NODES, object);
				if (describing != null) {
					this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configspecname).add(describing);
				}
			}

		} else if (PhenixUserCollectorObjectsHelper.isFromSoftypeEnum(object, PhenixUserCollectorObjectsHelper.DISCRIBING_END_OBJECTS.class)) {
			this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configspecname).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), object, object, true, true);
			this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configspecname).add(this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configspecname), object, parent,
					true, false);

			if (this.refreshing && this.getCollection(USERSCOLLECTIONS.MODIFIED).content.contains(parent)) {
				this.getCollection(USERSCOLLECTIONS.MODIFIED).add((WTObject) object);
			}

		} else if (!PhenixUserCollectorObjectsHelper.isFromSoftypeEnum(object, PhenixUserCollectorObjectsHelper.EXCLUDED_OBJECTS.class)) {
			this.getCollection(USERSCOLLECTIONS.NEUTRAL_NODES).getCollection(configspecname).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), object, object, true, true);
		}

		this.getCollection(USERSCOLLECTIONS.COLLECTED).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), object, parent, true, !reverse);

		if (this.recollect && !recollected) {
			this.getCollection(USERSCOLLECTIONS.TRANSVERSE_OBJECTS).add(this.getCollection(USERSCOLLECTIONS.COLLECTED), object, parent, true, !reverse);
		}

		if (this.refreshing) {
			WTSet removeds = null;

			if (configspecname == null && alreadyCollected) {
				Iterator<PhenixCollectorConfigSpec> it = this.configSpecs.iterator();
				while (it.hasNext()) {
					PhenixCollectorConfigSpec config1 = it.next();
					this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(config1.name).delete(object, false);
					this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(config1.name).delete(object, false);
				}
			}

			if (previous != null) {
				removeds = removeObjectFromCollector(previous, true);
				this.getCollection(USERSCOLLECTIONS.REMOVEDS).content.addAll(removeds);
				this.modificationMap.put(object, previous);

			} else if (!alreadyCollected) {
				this.modificationMap.put(object, null);
				if (parent != null && this.getCollection(USERSCOLLECTIONS.REVISED).content.contains(parent) && !reverse) {
					this.getCollection(USERSCOLLECTIONS.REVISED).add(object);
				} else if (!this.getCollection(USERSCOLLECTIONS.REMOVEDS).content.contains(object)) {
					WTObject original = PhenixCollectorWorkers.CollectMadeFrom(object);

					if (original != null && ((original instanceof WTPart && !((WTPart) original).getName().toLowerCase().contains("template"))
							|| (original instanceof EPMDocument && !((EPMDocument) original).getName().toLowerCase().contains("template")))) {
						this.getCollection(USERSCOLLECTIONS.MADEFROM).add(object);
						this.getCollection(USERSCOLLECTIONS.CHANGE_REF).add(this.getCollection(USERSCOLLECTIONS.MADEFROM), (WTObject) original, (WTObject) object, true, true);
						if (this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).content.contains(object)) {
							WTObject described = this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getRelated(this.getCollection(USERSCOLLECTIONS.END_NODES), object);
							if (described != null) {
								this.getCollection(USERSCOLLECTIONS.MADEFROM).add(described);
								this.getCollection(USERSCOLLECTIONS.ADDED).delete(described, false);
								this.getCollection(USERSCOLLECTIONS.CHANGE_REF).add(this.getCollection(USERSCOLLECTIONS.MADEFROM), (WTObject) original, (WTObject) described, true, true);
							}
						}

					} else {
						this.getCollection(USERSCOLLECTIONS.ADDED).add(object);
					}

				}

			}

			if (alreadyCollected) {
				WTList allchild = navigateObject(object, false, null);
				needDelete.removeAll(allchild);
			}

		}

		if (alreadyCollected && !recollected) {
			return;
		}
		if (this.recollect && !recollected) {
			return;
		}
		this.count++;

		if (recollected && previous != null && this.getCollection(USERSCOLLECTIONS.TRANSVERSE_OBJECTS).content.contains(previous)) {
			this.getCollection(USERSCOLLECTIONS.TRANSVERSE_OBJECTS).delete(previous, false);
			return;
		}

		collectBrowsableLinks(object, config);

		if (object instanceof EPMDocument) {
			if (PhenixUserCollectorObjectsHelper.isFromSoftypeEnum(object, PhenixUserCollectorObjectsHelper.DISCRIBING_END_OBJECTS.class)) {
				String collname = null;
				if (config != null && !config.name.equals(PhenixCollectorConfigSpec.DEFAULT_CONFIGSPEC_NAME)) {
					collname = config.name;

				}
				if (this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(collname).getRelated(this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(collname), object,
						true) == null) {
					PhenixCollectorWorkers.collectDescribed(this, (EPMDocument) object, config);
				}

			}

		}
	}

	private void addType(Persistable persistable) {
		TypeIdentifier typeIdentifier = TypeIdentifierHelper.getType(persistable);
		boolean added = this.uniqueTypes.add(typeIdentifier);
		if(added && LOGGER.isDebugEnabled()) {
			LOGGER.debug("Added unique type : " + typeIdentifier.getTypename());
		}
	}

	public void purgeCollector() {
		for (Object objectref : this.needDelete) {
			Object object = ((ObjectReference) objectref).getObject();
			removeObject(object);
		}
		this.needDelete.clear();
	}

	public WTSet removeObjectFromCollector(WTObject previous, boolean firstonly) {
		/* get all child */
		WTList allchild = navigateObject(previous, false, null);
		WTSet removeds = new WTHashSet();
		/* foreach child remove object if not use in node not is Set */
		int i = 0;
		for (Object objectref : allchild) {
			boolean toremove = true;
			Object object = ((ObjectReference) objectref).getObject();
			Set<PhenixCollectionlink> links = this.getCollection(USERSCOLLECTIONS.COLLECTED).objectslink.get(object);
			if (links != null) {
				for (PhenixCollectionlink link : links) {
					if (!previous.equals(object) && link.collection.name.equals(USERSCOLLECTIONS.COLLECTED.toString()) && !allchild.contains(link.obj) && link.reverse) {
						toremove = false;
					}

				}
			}

			if (toremove) {
				if ((i == 0 & firstonly) || !firstonly) {
					removeObject(object);
					removeds.add(object);
				} else {
					this.needDelete.add(object);
				}

			}

			i++;
		}
		return removeds;

	}

	public void removeObject(Object object) {
		/* remove object from other collections */
		Object master = null;
		this.count--;
		Set<PhenixCollectionlink> links = this.getCollection(USERSCOLLECTIONS.COLLECTED).objectslink.get(object);
		if (links != null) {
			Set<PhenixCollectionlink> forremove = new HashSet<PhenixCollectionlink>();

			for (PhenixCollectionlink link : links) {
				/* if same object only */
				if (link.obj.equals(object)) {
					forremove.add(link);
				}

				if (link.collection.name.equals(USERSCOLLECTIONS.MASTER)) {
					master = link.obj;
				}

			}
			for (PhenixCollectionlink link : forremove) {
				link.collection.delete((WTObject) link.obj, false);
			}

		}

		if (master != null) {
			for (Object childref : this.getCollection(USERSCOLLECTIONS.MASTER).getRelateds(USERSCOLLECTIONS.COLLECTED, (WTObject) master)) {
				Object child = ((ObjectReference) childref).getObject();
				this.getCollection(USERSCOLLECTIONS.COLLECTED).delete((WTObject) child, false);
			}
			this.getCollection(USERSCOLLECTIONS.MASTER).delete((WTObject) master, false);
		} else {
			this.getCollection(USERSCOLLECTIONS.COLLECTED).delete((WTObject) object, false);
		}

	}

	public WTList navigateObject(WTObject objentry, boolean reverse, WTList list) {
		if (list == null) {
			list = new WTArrayList();
		}
		list.add(objentry);
		WTSet objlink = this.getCollection(USERSCOLLECTIONS.COLLECTED).getRelateds(USERSCOLLECTIONS.COLLECTED, objentry, reverse);
		for (Object objectref : objlink) {

			Object object = ((ObjectReference) objectref).getObject();
			LOGGER.debug("collect  " + ((WTObject) object).getDisplayIdentity() + " from  " + objentry.getDisplayIdentity());
			navigateObject((WTObject) object, reverse, list);
		}
		return list;
	}

	public void filter() throws WTPropertyVetoException, WTException {

		String configSpecName = this.currentConfigSpecName;
		if (configSpecName.equals("Default")) {
			configSpecName = null;
		}

		this.getCollection(USERSCOLLECTIONS.FILTERED).clear();
		WTSet tofilter;
		if (this.docdriven) {
			tofilter = this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configSpecName).content;
		} else {
			tofilter = this.getCollection(USERSCOLLECTIONS.CONF_NODES).content;
		}

		LOGGER.info("Start filter on :" + tofilter.size() + " Objects");
		Iterator<String> iterator = this.appli.iterator();
		WTSet from = tofilter;
		WTSet filtered = null;
		WTSet allfiltered = new WTHashSet();
		while (iterator.hasNext()) {
			String applistr = iterator.next();

			filtered = AdvancedExpressionHelper.service.filters(from, applistr, true);
			LOGGER.info("Filter " + from.size() + " objects on appli :" + applistr + " return   :" + filtered.size() + " Objects");
			if (this.appliand) {
				from = filtered;
			} else {
				allfiltered.addAll(filtered);
			}

		}

		if (this.appliand) {
			allfiltered.addAll(filtered);
		}

		for (Object objectref : allfiltered) {
			Object object = ((ObjectReference) objectref).getObject();
			this.getCollection(USERSCOLLECTIONS.FILTERED).add((WTObject) object);
			WTObject described = this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configSpecName)
					.getRelated(this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configSpecName), object);

			if (described != null) {
				this.getCollection(USERSCOLLECTIONS.FILTERED).add(described);
			}
		}
		propagateFilter();
		if (this.docdriven) {
			this.getCollection(USERSCOLLECTIONS.FILTERED).addAll(this.getCollection(USERSCOLLECTIONS.CONF_NODES).getCollection(configSpecName));
		} else {
			this.getCollection(USERSCOLLECTIONS.FILTERED).addAll(this.getCollection(USERSCOLLECTIONS.END_NODES).getCollection(configSpecName));
			this.getCollection(USERSCOLLECTIONS.FILTERED).addAll(this.getCollection(USERSCOLLECTIONS.DISCRIBING_END_NODES).getCollection(configSpecName));
		}

		this.getCollection(USERSCOLLECTIONS.FILTERED).addAll(this.getCollection(USERSCOLLECTIONS.NEUTRAL_NODES).getCollection(configSpecName));

	}

	public void propagateFilter() {

		this.getCollection(USERSCOLLECTIONS.FILTEREDBYLINK).clear();
		this.getCollection(USERSCOLLECTIONS.FILTEREDBYLINK).addAll(this.getCollection(USERSCOLLECTIONS.FILTERED));

		for (Object objectref : this.getCollection(USERSCOLLECTIONS.FILTERED).content) {
			Object object = ((ObjectReference) objectref).getObject();
			WTList list;
			list = navigateObject((WTObject) object, false, null);
			list = navigateObject((WTObject) object, true, list);
			this.getCollection(USERSCOLLECTIONS.FILTEREDBYLINK).addAll(list);
		}
	}

	public void paramInits(ComponentParams params) {
		boolean previousdocdriven;
		boolean previouspropagate;
		boolean previousappliand;

		Set<String> previousappli = new HashSet<String>();

		String previousshowmode;
		String tableId = "ext.renault.phenix.mvc.builders.PhenixCollectorTableBuilder";
		String appli1 = (String) params.getParameter("appli1");
		String appli2 = (String) params.getParameter("appli2");
		String appli3 = (String) params.getParameter("appli3");
		String number = (String) params.getParameter("numberToSearch");
		String appliand = (String) params.getParameter("appliand");
		String docDriven = (String) params.getParameter("docdriven");
		String propagate = (String) params.getParameter("propagate");
		String showmode = (String) params.getParameter("showmode");
		String autorefresh = (String) params.getParameter("autorefresh");
		String configspec = (String) params.getParameter("configspec");

		previousappli.addAll(this.appli);
		previousappliand = this.appliand;
		previousdocdriven = this.docdriven;
		this.tofilter = false;

		this.displayChanged = false;
		if (number != null && !number.isEmpty()) {
			this.number = number;
		} else {
			this.number = null;
		}

		if (appli1 != null || appli2 != null || appli3 != null) {
			this.appli.clear();
			if (!appli1.isEmpty()) {
				this.appli.add(appli1);
			}

			if (!appli2.isEmpty()) {
				this.appli.add(appli2);
			}

			if (!appli3.isEmpty()) {
				this.appli.add(appli3);
			}

		}
		previousappliand = this.appliand;
		if (appliand != null) {

			if (appliand.equals("true")) {
				this.appliand = true;
			} else {
				this.appliand = false;
			}

		}

		previousdocdriven = this.docdriven;
		if (docDriven != null) {

			if (docDriven.equals("true")) {
				this.docdriven = true;
			} else {
				this.docdriven = false;
			}

		}

		previouspropagate = this.propagate;
		if (propagate != null) {
			if (propagate.equals("true")) {
				this.propagate = true;
			} else {
				this.propagate = false;
			}

		}

		if (configspec != null) {
			this.currentConfigSpecName = configspec;

		}

		previousshowmode = this.showmode;
		if (showmode != null) {
			this.showmode = showmode;
		}

		if (previouspropagate != this.propagate || !previousshowmode.equals(this.showmode)) {
			this.displayChanged = true;
		}

		if (autorefresh != null) {
			if (autorefresh.equals("true")) {
				this.autorefresh = true;
			} else {
				this.autorefresh = false;
			}
		}

		if (previousdocdriven != this.docdriven || previousappli.size() != this.appli.size() || !previousappli.containsAll(this.appli) || previousappliand != this.appliand || this.autorefresh) {
			this.tofilter = true;
		}

	}

	public void collectBrowsableLinks(WTObject o, PhenixCollectorConfigSpec config) {

		for (PhenixBrowsableLink bl : this.browsableLinksService.getCurrentBrowsableLinks()) {
			Class[] cArg = null;

			if (bl.linkType != null) {
				cArg = new Class[4];
				cArg[0] = PhenixUserCollector.class;
				cArg[1] = WTObject.class;
				cArg[2] = PhenixCollectorConfigSpec.class;
				cArg[3] = TypeIdentifier.class;
			} else {
				cArg = new Class[3];
				cArg[0] = PhenixUserCollector.class;
				cArg[1] = WTObject.class;
				cArg[2] = PhenixCollectorConfigSpec.class;
			}

			try {
				Method m = PhenixCollectorWorkers.class.getDeclaredMethod(bl.method, cArg);

				if (bl.linkType != null) {
					m.invoke(null, this, o, config, bl.linkType);
				} else {
					m.invoke(null, this, o, config);
				}
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException | SecurityException e) {

				e.printStackTrace();
			}
		}

	}

	public void clearChanges() throws WTException, WTPropertyVetoException {
		this.getCollection(USERSCOLLECTIONS.MODIFIED).clear();
		this.getCollection(USERSCOLLECTIONS.ADDED).clear();
		this.getCollection(USERSCOLLECTIONS.REVISED).clear();
		this.getCollection(USERSCOLLECTIONS.MADEFROM).clear();
		this.getCollection(USERSCOLLECTIONS.TRANSVERSE_OBJECTS).clear();
		this.extended = false;
		if (this.tracker != null) {
			this.tracker.save();
		}

	}

	private void initDefaultBrowsableLinks() throws WTException {

		WTUser user = (WTUser) SessionHelper.manager.getPrincipal();
		OrgContainer orgContainer = WTContainerHelper.service.getOrgContainer(user.getOrganization());

		String defaultBrowsableLinksPref = (String) PreferenceHelper.service.getValue(orgContainer.getContainerReference(), "PhenixCollectorDefaultBrowsableLinks",
				PreferenceClient.WINDCHILL_CLIENT_NAME, user);
		ArrayList<PhenixBrowsableLink> currentSelectedBrowsableLinks=browsableLinksService.getCurrentBrowsableLinks();
		
		if (defaultBrowsableLinksPref != null && !defaultBrowsableLinksPref.isEmpty() &&currentSelectedBrowsableLinks.isEmpty()) {
			String[] internalValues = defaultBrowsableLinksPref.split(",");
			for (String internalValue : internalValues) {

				String value = internalValue;
				String displayValue = null;
				TypeIdentifier linkType = null;
				if (value.indexOf('/') != -1) {
					String[] values = value.split("/");
					value = values[0];
					linkType = TypeDefinitionServiceHelper.service.getTypeIdentifier(values[1]);
					if (linkType == null) {
						value = null;
					} else {
						displayValue = TypeDefinitionServiceHelper.service.getTypeDefView(linkType).getDisplayName();
					}
				}

				for (BROWSABLE_LINKS bl : PhenixCollectorWorkers.BROWSABLE_LINKS.values()) {
					if (bl.name().equals(value) && (linkType == null || bl.linkTypePossible)) {
						if (displayValue == null) {
							WTMessage m = new WTMessage(RESOURCE, value, null);
							displayValue = m.toString();
						}
						this.browsableLinksService.addCurrentBrowsableLink(new PhenixBrowsableLink(displayValue, internalValue, bl.method));
					}
				}
			}
		} else if (!currentSelectedBrowsableLinks.isEmpty()) {

			//String[] internalValues = defaultBrowsableLinksPref.split(",");
			for (PhenixBrowsableLink phenixBrowsableLink : currentSelectedBrowsableLinks) {

				String value = phenixBrowsableLink.getInternalName();
				LOGGER.debug("Calling from initDefaultBrowsableLinks Link Internal name  ===> "+value);
				LOGGER.debug("Calling from initDefaultBrowsableLinks Link Display name  ===> "+phenixBrowsableLink.displayName);
				String displayValue = null;
				TypeIdentifier linkType = null;
				if (value.indexOf('/') != -1) {
					String[] values = value.split("/");
					value = values[0];
					linkType = TypeDefinitionServiceHelper.service.getTypeIdentifier(values[1]);
					if (linkType == null) {
						value = null;
					} else {
						displayValue = TypeDefinitionServiceHelper.service.getTypeDefView(linkType).getDisplayName();
					}
				}

				for (BROWSABLE_LINKS bl : PhenixCollectorWorkers.BROWSABLE_LINKS.values()) {
					if (bl.name().equals(value) && (linkType == null || bl.linkTypePossible)) {
						if (displayValue == null) {
							WTMessage m = new WTMessage(RESOURCE, value, null);
							displayValue = m.toString();
						}
						this.browsableLinksService.addCurrentBrowsableLink(new PhenixBrowsableLink(displayValue, phenixBrowsableLink.getInternalName(), bl.method));
					}
				}
			}
		
		}
		
		else
		
		{
			for (BROWSABLE_LINKS defaultbl : PhenixCollectorWorkers.DEFAULT_BROWSABLE_LINKS) {
				WTMessage m = new WTMessage(RESOURCE, defaultbl.name(), null);
				this.browsableLinksService.addCurrentBrowsableLink(new PhenixBrowsableLink(m.toString(), defaultbl.name(), defaultbl.method));
			}
		}
	}

	public Set<TypeIdentifier> getUniqueTypes() {
		return uniqueTypes;
	}

	public void setUniqueTypes(Set<TypeIdentifier> uniqueTypes) {
		this.uniqueTypes = uniqueTypes;
	}

}
